"""Composer trigger rules (`/commands`, `$skills`) as pure text functions."""
import json
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "src/pa/server/static/js/composer-triggers.js"


def run(expression: str):
    program = (
        'const fs=require("fs"),vm=require("vm");global.window={};'
        'vm.runInThisContext(fs.readFileSync(process.argv[1],"utf8"));const T=window.PAComposerTriggers;'
        "console.log(JSON.stringify(" + expression + "));"
    )
    done = subprocess.run(["node", "-e", program, str(SCRIPT)], capture_output=True, text=True, check=True)
    return json.loads(done.stdout)


def find(text, caret=None, enabled=("/", "$")):
    return run(f"T.find({json.dumps(text)}, {json.dumps(caret)}, {json.dumps(list(enabled))})")


def test_slash_opens_only_at_the_start_of_the_message():
    assert find("/rev") == {"trigger": "/", "start": 0, "end": 4, "query": "rev"}
    assert find("/") == {"trigger": "/", "start": 0, "end": 1, "query": ""}
    assert find("please /rev") is None
    assert find("line one\n/rev") is None


def test_slash_closes_once_arguments_start_and_for_the_literal_escape():
    assert find("/rename new name") is None
    assert find("//literal") is None
    # The caret inside the argument is not on the command word.
    assert find("/rename foo", 11) is None


def test_dollar_opens_at_any_word_start_but_not_inside_words():
    assert find("$ima")["query"] == "ima"
    assert find("use $seed")["start"] == 4
    assert find("price is 5$") is None
    assert find("a$b") is None
    assert find("$google-drive:google-docs")["query"] == "google-drive:google-docs"


def test_dollar_rejects_text_that_cannot_be_a_skill_name():
    assert find("$5,000") is None
    assert find("$(echo hi)") is None


def test_the_word_under_the_caret_is_replaced_as_a_whole():
    found = find("use $se|ed now".replace("|", ""), 7)
    assert found == {"trigger": "$", "start": 4, "end": 9, "query": "seed"}
    assert run('T.replace("use $seed now", {start:4,end:9}, "$seed-run")') == {"text": "use $seed-run now", "caret": 14}
    assert run('T.replace("/re", {start:0,end:3}, "/review")') == {"text": "/review ", "caret": 8}


def test_disabled_triggers_are_ignored():
    assert find("$seed", None, ("/",)) is None
    assert find("/plan", None, ("$",)) is None


def test_ranking_prefers_prefix_then_word_part_then_substring_then_description():
    items = [
        {"name": "$openai-docs", "description": "Codex pricing"},
        {"name": "$google-docs", "description": "Docs"},
        {"name": "$docs-tool", "description": ""},
        {"name": "$xdocsx", "description": ""},
        {"name": "$unrelated", "description": "reads the docs aloud"},
        {"name": "$none", "description": ""},
    ]
    ranked = run(f'T.rank({json.dumps(items)}, "docs", {{strip: "$"}}).map(i => i.name)')
    assert ranked == ["$docs-tool", "$google-docs", "$openai-docs", "$xdocsx", "$unrelated"]
    assert run(f'T.rank({json.dumps(items)}, "", {{strip: "$"}}).length') == 6


def test_at_opens_at_word_starts_so_email_addresses_are_ordinary_text():
    assert find("@src/app", None, ("@",))["query"] == "src/app"
    assert find("see @card:fix please", 13, ("@",))["query"] == "card:fix"
    assert find("mail ada@example.com", None, ("@",)) is None
    assert find("@", None, ("@",))["query"] == ""


def test_bang_makes_the_whole_message_a_shell_command_and_double_bang_flips_the_choice():
    assert run('T.shell("!git status -s")') == {"flip": False, "command": "git status -s"}
    assert run('T.shell("!!  ls | head")') == {"flip": True, "command": "ls | head"}
    assert run('T.shell("!")') == {"flip": False, "command": ""}
    assert run('T.shell("run !ls")') is None
    assert run('T.shell("hello!")') is None


def test_backslash_bang_is_the_escape_for_literal_text():
    assert run(r'T.unescapeShell("\\!important")') == "!important"
    assert run('T.unescapeShell("!important")') == "!important"
    assert run('T.unescapeShell("plain")') == "plain"
