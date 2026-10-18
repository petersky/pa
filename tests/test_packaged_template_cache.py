import base64
import json
from unittest.mock import patch

import pytest
from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from pa.server.template_cache import PackagedTemplateCache, build_template_cache


def fixture(tmp_path):
    source = tmp_path / 'templates'
    source.mkdir()
    (source / 'page.html').write_text('{% include "part.html" %}{{ value }}')
    (source / 'part.html').write_text('{{ "literal"|upper }}:')
    manifest = tmp_path / 'cache.json'
    build_template_cache(source, manifest)
    return source, manifest


def environment(source, manifest, **kwargs):
    return Environment(loader=FileSystemLoader(source), autoescape=select_autoescape(),
                       bytecode_cache=PackagedTemplateCache(manifest), **kwargs)


def test_complete_includes_and_escaping_match_without_runtime_compilation_or_writes(tmp_path):
    source, manifest = fixture(tmp_path)
    before = manifest.read_bytes(), manifest.stat().st_mtime_ns
    expected = Environment(loader=FileSystemLoader(source), autoescape=select_autoescape()).get_template('page.html').render(value='<script>é</script>')
    env = environment(source, manifest)
    with patch.object(env, 'compile', side_effect=AssertionError('unexpected runtime compilation')):
        assert env.get_template('page.html').render(value='<script>é</script>') == expected
    assert (manifest.read_bytes(), manifest.stat().st_mtime_ns) == before


def test_source_edit_invalidates_the_packaged_code(tmp_path):
    source, manifest = fixture(tmp_path)
    (source / 'part.html').write_text('edited:')
    env = environment(source, manifest)
    with patch.object(env, 'compile', wraps=env.compile) as compile:
        assert env.get_template('page.html').render(value='current') == 'edited:current'
    assert compile.call_count == 1


def test_plugin_loader_precedence_is_preserved(tmp_path):
    source, manifest = fixture(tmp_path)
    plugin = tmp_path / 'plugin'
    plugin.mkdir()
    (plugin / 'part.html').write_text('plugin:')
    env = environment([plugin, source], manifest)
    assert env.get_template('page.html').render(value='value') == 'plugin:value'


@pytest.mark.parametrize('field', ['python', 'jinja', 'version'])
def test_incompatible_artifact_metadata_uses_the_normal_compiler(tmp_path, field):
    source, manifest = fixture(tmp_path)
    value = json.loads(manifest.read_text())
    value[field] = 'different'
    manifest.write_text(json.dumps(value))
    env = environment(source, manifest)
    with patch.object(env, 'compile', wraps=env.compile) as compile:
        assert env.get_template('page.html').render(value='fresh') == 'LITERAL:fresh'
    assert compile.call_count == 2


def test_changed_filter_constant_folding_uses_current_environment(tmp_path):
    source, manifest = fixture(tmp_path)
    env = environment(source, manifest)
    env.filters['upper'] = lambda value: 'replacement'
    assert env.get_template('page.html').render(value='current') == 'replacement:current'


def test_strict_undefined_environment_keeps_its_contract(tmp_path):
    source, manifest = fixture(tmp_path)
    env = environment(source, manifest, undefined=StrictUndefined)
    with patch.object(env, 'compile', wraps=env.compile) as compile:
        assert env.get_template('page.html').render(value='current') == 'LITERAL:current'
    assert compile.call_count == 2


@pytest.mark.parametrize('content', ['not-json', '[]', '{}'])
def test_missing_or_invalid_manifest_falls_back(tmp_path, content):
    source, manifest = fixture(tmp_path)
    manifest.write_text(content)
    assert environment(source, manifest).get_template('page.html').render(value='ok') == 'LITERAL:ok'
    manifest.unlink()
    assert environment(source, manifest).get_template('page.html').render(value='ok') == 'LITERAL:ok'


def test_corrupt_bytecode_falls_back_to_source(tmp_path):
    source, manifest = fixture(tmp_path)
    value = json.loads(manifest.read_text())
    value['entries'] = {key: base64.b64encode(b'invalid-code').decode() for key in value['entries']}
    manifest.write_text(json.dumps(value))
    assert environment(source, manifest).get_template('page.html').render(value='ok') == 'LITERAL:ok'


def test_build_output_is_independent_of_temporary_build_root(tmp_path):
    first = tmp_path / 'first'
    second = tmp_path / 'second'
    first.mkdir()
    second.mkdir()
    _, first_manifest = fixture(first)
    _, second_manifest = fixture(second)
    assert first_manifest.read_bytes() == second_manifest.read_bytes()


@pytest.mark.parametrize('name', ['page.html', 'page.txt', 'page.xml'])
def test_default_autoescape_for_each_extension_matches_source(tmp_path, name):
    source = tmp_path / 'templates'
    source.mkdir()
    (source / name).write_text('{{ value }}')
    manifest = tmp_path / 'cache.json'
    build_template_cache(source, manifest)
    expected = Environment(loader=FileSystemLoader(source), autoescape=select_autoescape()).get_template(name).render(value='<tag>')
    env = environment(source, manifest)
    with patch.object(env, 'compile', side_effect=AssertionError('unexpected compilation')):
        assert env.get_template(name).render(value='<tag>') == expected


def test_custom_autoescape_and_finalize_keep_runtime_semantics(tmp_path):
    source, manifest = fixture(tmp_path)
    env = Environment(loader=FileSystemLoader(source), autoescape=False,
                      finalize=lambda value: str(value).replace('literal', 'changed'),
                      bytecode_cache=PackagedTemplateCache(manifest))
    with patch.object(env, 'compile', wraps=env.compile) as compile:
        assert env.get_template('page.html').render(value='<tag>') == 'LITERAL:<tag>'
    assert compile.call_count == 2
