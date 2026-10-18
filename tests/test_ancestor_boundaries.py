"""Full fork ancestry retains exact indexed boundaries without a ready-tip copy."""
from unittest.mock import patch

import pytest

from pa.core.performance import OperationObservation, current_observation
from pa.sync.event_log import EventHistoryLimitError, EventHistoryObjectError
from tests.test_ancestry_membership import history


def test_full_fork_ancestry_transfers_only_its_exact_boundary(tmp_path):
    log, objects, retained, commit = history(tmp_path)
    fork = commit('fork', [retained[-2]])
    original = log.index.ancestors
    original_get = objects.get

    def ancestry(head):
        assert head != retained[-1], 'copied unrelated ready-tip ancestry'
        return original(head)

    def novel_only(digest):
        assert digest not in retained, 'reopened verified retained history'
        return original_get(digest)

    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with patch.object(log.index, 'ancestors', side_effect=ancestry), \
             patch.object(objects, 'get', side_effect=novel_only):
            assert log._ancestors(fork) == set(retained[:-1]) | {fork}
        assert observation.sqlite_aggregate_items == len(retained) - 1
    finally:
        current_observation.reset(token)


def test_multiple_boundaries_union_exact_history_and_novel_commits(tmp_path):
    log, _, retained, commit = history(tmp_path)
    left = commit('left', [retained[15]])
    right = commit('right', [retained[45]])
    merge = commit('merge', [left, right])
    assert log._ancestors(merge) == set(retained[:46]) | {left, right, merge}


def test_index_reset_during_boundary_discovery_uses_canonical_fallback(tmp_path):
    log, _, retained, commit = history(tmp_path)
    fork = commit('fork', [retained[-2]])
    original = log._iter_commits_parent_first
    calls = []

    def reset_then_walk(*args, **kwargs):
        calls.append(kwargs.get('stop'))
        if len(calls) == 1:
            log.index.reset_realm('default')
        yield from original(*args, **kwargs)

    with patch.object(log, '_iter_commits_parent_first', side_effect=reset_then_walk):
        assert log._ancestors(fork) == set(retained[:-1]) | {fork}
    assert len(calls) == 2 and calls[0] is not None and calls[1] is None


def test_incomplete_index_and_missing_parent_keep_canonical_validation(tmp_path):
    log, _, retained, commit = history(tmp_path)
    fork = commit('fork', [retained[-2]])
    log.index.mark_failed('default', 'fixture incomplete')
    assert log._ancestors(fork) == set(retained[:-1]) | {fork}
    missing = commit('missing', ['f' * 64])
    with pytest.raises(EventHistoryObjectError):
        log._ancestors(missing)


def test_combined_history_limit_and_ready_head_race_are_preserved(tmp_path):
    log, _, retained, commit = history(tmp_path)
    fork = commit('fork', [retained[-2]])
    with patch('pa.sync.event_log.MAX_HISTORY_COMMITS', 40):
        with pytest.raises(EventHistoryLimitError):
            log._ancestors(fork)
    original = log.index.ancestors
    calls = []

    def missed_first(head):
        calls.append(head)
        return None if len(calls) == 1 else original(head)

    with patch.object(log.index, 'ancestors', side_effect=missed_first):
        assert log._ancestors(retained[-1]) == set(retained)
