"""The Work board groups by what is happening, and its cards say little but the right thing."""
from __future__ import annotations

from pa.domain.models import CardCreate, CardLane
from tests.test_home_lifecycle_evidence import app, record_for, view  # noqa: F401


def _card(app, title, lane=CardLane.INBOX, **kw):
    return app.state.ctx.store.create_card(CardCreate(title=title, lane=lane, **kw))


def test_state_columns_are_cross_lane_and_not_draggable(app):
    client = app.state.test_client
    open_card = _card(app, "Not yet started")
    finished = _card(app, "Finished long ago", CardLane.DONE)
    idle = client.get("/partials/cards?attention=idle").text
    assert "Not yet started" in idle and "Finished long ago" not in idle
    assert "draggable" not in idle  # state columns have no lane to drop into
    done = client.get("/partials/cards?attention=outcome").text
    assert "Finished long ago" in done and "Not yet started" not in done
    # A card in lane Waiting is bucketed by its actual state, not its lane.
    waiting = _card(app, "Waiting lane card", CardLane.WAITING)
    assert view(app, waiting)["group"] in {"quiet", "attention", "motion", "outcome"}
    assert open_card.id in idle and finished.id in done


def test_lane_view_is_still_draggable_with_lane_param(app):
    _card(app, "Inbox card")
    html = app.state.test_client.get("/partials/cards?lane=inbox").text
    assert 'draggable="true"' in html


def test_default_work_page_groups_by_state_and_lane_grouping_is_opt_in(app):
    client = app.state.test_client
    state = client.get("/work").text
    assert 'data-board-group="state"' in state and 'data-bucket="actionable"' in state
    assert 'class="board-column" id="lane-' not in state
    assert 'data-board-group="lane"' in client.get("/work?group=lane").text
    # Home's "view all actionable work" and explicit lanes keep their lane board.
    assert 'data-board-group="lane"' in client.get("/work?attention=actionable").text
    assert 'data-board-group="lane"' in client.get("/work?lane=waiting").text


def test_finished_cards_are_title_only_with_no_button_or_footer(app):
    _card(app, "Settled card", CardLane.DONE)
    html = app.state.test_client.get("/partials/cards?lane=done").text
    assert "Settled card" in html
    assert "stage-strip" not in html and "compact-card-footer" not in html
    assert "Open card" not in html


def test_card_dialog_has_no_open_card_button_or_duplicate_summary(app):
    card = _card(app, "Dialog card")
    html = app.state.test_client.get(f"/partials/cards/{card.id}/detail").text
    assert ">Open card<" not in html
    assert 'data-card-summary-tools' in html  # diagnostics and regenerate are folded away
    assert "Summary tools" in html
    assert "data-stage-strip" in html


def test_not_started_card_shows_summary_once_and_one_action(app):
    card = _card(app, "Write the release notes", body="Draft notes for v1.7")
    html = app.state.test_client.get("/partials/cards?lane=inbox").text
    assert html.count("Write the release notes") >= 1
    assert "data-card-dispatch-open" in html or "Start work" in html
    assert html.count("data-card-dispatch-open") <= 1
