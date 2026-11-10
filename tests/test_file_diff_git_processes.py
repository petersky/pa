"""Combined root discovery keeps fresh revision/commit priority and guards."""
from pathlib import Path
import subprocess
from unittest.mock import patch

import pytest

from pa.modules.files import _default_diff_base, _file_diff


def git(repo, *args):
    return subprocess.run(["git","-C",str(repo),*args],capture_output=True,text=True,check=True)


@pytest.fixture
def repo(tmp_path):
    git(tmp_path,"init","-q")
    (tmp_path/"sample.py").write_text("first\n")
    git(tmp_path,"add","sample.py")
    git(tmp_path,"-c","user.name=Fixture","-c","user.email=fixture@example.invalid",
        "-c","commit.gpgSign=false","commit","-qm","fixture")
    return tmp_path


def legacy(repo):
    for name in ("origin/main","origin/master","HEAD"):
        value = subprocess.run(["git","-C",str(repo),"rev-parse","--verify",name+"^{commit}"],
                               capture_output=True,text=True,timeout=3,check=False)
        if value.returncode == 0:
            return name
    return "HEAD"


def test_head_only_default_diff_avoids_a_separate_root_process(repo):
    original = subprocess.run
    with patch("pa.modules.files.subprocess.run",wraps=original) as calls:
        assert _file_diff(repo/"sample.py",None)[1] == "HEAD"
    assert calls.call_count == 4
    assert [call.kwargs["timeout"] for call in calls.call_args_list] == [3,3,3,5]


@pytest.mark.parametrize("main,master,expected",[
    (None,None,"HEAD"),(None,"commit","origin/master"),("tree","commit","origin/master"),
    ("blob","tree","HEAD"),("commit","commit","origin/main"),
    ("tag","commit","origin/main"),("symbolic","commit","origin/main"),
])
def test_priority_and_commit_peeling_match_existing_verification(repo,main,master,expected):
    for name,kind in (("main",main),("master",master)):
        if kind is None:
            continue
        ref=f"refs/remotes/origin/{name}"
        if kind == "symbolic":
            git(repo,"symbolic-ref",ref,"HEAD")
        else:
            target={"commit":"HEAD","tree":"HEAD^{tree}","blob":"HEAD:sample.py","tag":"refs/tags/peeled"}[kind]
            if kind == "tag":
                git(repo,"-c","user.name=Fixture","-c","user.email=fixture@example.invalid",
                    "tag","-a","peeled","-m","fixture")
            git(repo,"update-ref",ref,git(repo,"rev-parse",target).stdout.strip())
    assert _default_diff_base(repo) == legacy(repo) == expected
    assert _file_diff(repo/"sample.py",None)[1] == expected


def test_dwim_tag_name_and_packed_refs_keep_existing_priority(repo):
    git(repo,"tag","origin/main")
    git(repo,"update-ref","refs/remotes/origin/master","HEAD")
    git(repo,"pack-refs","--all")
    assert _default_diff_base(repo) == legacy(repo) == "origin/main"


def test_unborn_repo_keeps_head_fallback(tmp_path):
    git(tmp_path,"init","-q")
    assert _default_diff_base(tmp_path) == legacy(tmp_path) == "HEAD"


def test_ref_changes_are_not_cached_and_default_diff_matches_explicit(repo):
    path=repo/"sample.py";path.write_text("second\n")
    assert _default_diff_base(repo) == "HEAD"
    git(repo,"update-ref","refs/remotes/origin/master","HEAD")
    assert _default_diff_base(repo) == "origin/master"
    git(repo,"update-ref","refs/remotes/origin/main","HEAD")
    assert _default_diff_base(repo) == "origin/main"
    assert _file_diff(path,None)[0] == _file_diff(path,"origin/main")[0]


def test_explicit_base_skips_default_selection(repo):
    with patch("pa.modules.files._default_diff_base",side_effect=AssertionError("default lookup")):
        assert _file_diff(repo/"sample.py","HEAD")[1] == "HEAD"


def test_first_candidate_success_never_probes_lower_priority_refs(repo):
    git(repo,"update-ref","refs/remotes/origin/main","HEAD")
    original = subprocess.run
    with patch("pa.modules.files.subprocess.run",wraps=original) as calls:
        assert _file_diff(repo/"sample.py",None)[1] == "origin/main"
    assert calls.call_count == 2
    assert all("origin/master^{commit}" not in call.args[0] and "HEAD^{commit}" not in call.args[0]
               for call in calls.call_args_list)


@pytest.mark.parametrize("main",[False,True])
def test_combined_path_keeps_root_names_with_newlines(tmp_path,main):
    folder=tmp_path/"line\nbreak";folder.mkdir()
    git(folder,"init","-q")
    if main:
        (folder/"f").write_text("fixture")
        git(folder,"add","f")
        git(folder,"-c","user.name=Fixture","-c","user.email=fixture@example.invalid",
            "-c","commit.gpgSign=false","commit","-qm","fixture")
        git(folder,"update-ref","refs/remotes/origin/main","HEAD")
    assert _file_diff(folder/"f",None) == ("","origin/main" if main else "HEAD")


def test_non_repository_and_outside_reported_root_never_run_diff(tmp_path):
    original = subprocess.run
    with patch("pa.modules.files.subprocess.run",wraps=original) as calls:
        assert _file_diff(tmp_path/"f",None) == ("",None)
    assert calls.call_count == 1
    with patch("pa.modules.files.subprocess.run",return_value=subprocess.CompletedProcess(
            [],0,str(tmp_path/"elsewhere")+"\n"+"a"*40+"\n")) as calls:
        (tmp_path/"elsewhere").mkdir()
        assert _file_diff(tmp_path/"f",None) == ("",None)
    assert calls.call_count == 1


def test_unknown_combined_output_uses_original_verification(repo):
    original = subprocess.run
    count = 0
    def execute(*args,**kwargs):
        nonlocal count
        count += 1
        if count == 1:
            return subprocess.CompletedProcess(args[0],0,"unexpected output\n","")
        return original(*args,**kwargs)
    with patch("pa.modules.files.subprocess.run",side_effect=execute):
        assert _file_diff(repo/"sample.py",None)[1] == "HEAD"
    assert count == 6


def test_timeout_propagates_without_retries_or_larger_deadline(repo):
    with patch("pa.modules.files.subprocess.run",side_effect=subprocess.TimeoutExpired("git",3)) as calls:
        with pytest.raises(subprocess.TimeoutExpired):
            _file_diff(repo/"sample.py",None)
    assert calls.call_count == 1
    assert calls.call_args.kwargs["timeout"] == 3


