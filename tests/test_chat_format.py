"""Display-only bubble formatting: JSON readability, media routing, retained images."""
import base64
import json
import subprocess
from pathlib import Path

from pa.instance.quiesce import ImageAttachment
from pa.modules.agent_chat import _store_chat_images

SCRIPT = Path(__file__).parents[1] / "src/pa/server/static/js/chat-format.js"


def run(expression: str):
    program = (
        'const fs=require("fs"),vm=require("vm");global.window={location:{href:"http://pa.test/agent"},'
        'PALinks:{filesystemTarget:r=>/^\\/(Users|tmp)\\//.test(r)?{path:r}:null}};global.document={};'
        "window.window=window;global.URL=URL;"
        'vm.runInThisContext(fs.readFileSync(process.argv[1],"utf8"));const F=window.PAChatFormat;'
        "console.log(JSON.stringify(" + expression + "));"
    )
    done = subprocess.run(["node", "-e", program, str(SCRIPT)], capture_output=True, text=True, check=True)
    return json.loads(done.stdout)


def prepare(text: str) -> str:
    return run("F.prepare(" + json.dumps(text) + ")")


def test_embedded_json_is_fenced_and_surrounding_prose_kept():
    result = prepare('The config is: {"name": "pa", "enabled": true, "retries": 3} and that is all.')
    assert result.startswith("The config is: \n\n```json\n{\n")
    assert '"retries": 3' in result
    assert result.endswith("and that is all.")


def test_large_integers_survive_pretty_printing():
    assert "12345678901234567890" in prepare('{"id": 12345678901234567890, "name": "exact-value"}')


def test_strings_containing_json_syntax_are_not_reformatted():
    pretty = run('F.prettyJson(' + json.dumps('{"a":"x, y: {z}"}') + ")")
    assert pretty == '{\n  "a": "x, y: {z}"\n}'


def test_existing_code_fences_are_untouched():
    text = '```python\nprint({"untouched": True, "more": "padding padding"})\n```'
    assert prepare(text) == text


def test_inline_code_short_brackets_and_links_are_left_alone():
    for text in (
        'Use `{"name": "pa", "enabled": true, "retries": 3}` here',
        "See reference [1] and [2].",
        "A [link](https://example.com) stays a link.",
        '{"unterminated": "object", "that": "never closes"',
    ):
        assert prepare(text) == text


def test_file_scheme_images_are_made_routable():
    assert prepare("![c](file:///tmp/c.png)") == "![c](/tmp/c.png)"


def test_media_kinds_and_urls():
    assert run('["/a/b.PNG","x.mp4?t=1","y.flac","z.txt"].map(F.mediaKind)') == ["image", "video", "audio", None]
    assert run('F.mediaUrl("/tmp/a b.mp3")') == "/api/files/raw?path=%2Ftmp%2Fa%20b.mp3"
    assert run('F.mediaUrl("https://e.test/a.mp4")') == "https://e.test/a.mp4"
    assert run('F.mediaUrl("javascript:alert(1)")') is None


def test_chat_images_are_stored_once_under_their_hash(tmp_path):
    data = b"\x89PNG-bytes"
    image = ImageAttachment(name="a.png", mime_type="image/png", data=base64.b64encode(data).decode())
    assert image.public_dict()["sha256"] == image.sha256
    root = tmp_path / "chat-images" / "session"
    _store_chat_images(root, [image, image])
    assert [path.name for path in root.iterdir()] == [image.sha256 + ".png"]
    assert (root / (image.sha256 + ".png")).read_bytes() == data
