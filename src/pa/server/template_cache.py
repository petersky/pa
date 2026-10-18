"""Read-only build-time Jinja bytecode, with the source loader as authority.

The wheel carries code compiled from its own templates. Jinja still reads each
selected source and checks its checksum; plugin precedence and edited sources
therefore retain the normal loader behavior. Unknown environments compile in the
usual way. No runtime cache file is written.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import pickle
import sys
from pathlib import Path
from types import CodeType

import jinja2
from jinja2 import BytecodeCache, Environment, FileSystemLoader, Undefined, select_autoescape
from jinja2.compiler import CodeGenerator
from jinja2.defaults import DEFAULT_FILTERS, DEFAULT_POLICIES, DEFAULT_TESTS

_AUTOESCAPE = select_autoescape()
_POLICIES = copy.deepcopy(DEFAULT_POLICIES)


def _default_autoescape(value) -> bool:
    # Starlette creates a fresh select_autoescape closure for each app. Accept
    # only that exact implementation and its default immutable closure values;
    # custom autoescape functions keep their normal compilation behavior.
    return (getattr(value, '__code__', None) is _AUTOESCAPE.__code__
            and getattr(value, '__globals__', None) is _AUTOESCAPE.__globals__
            and tuple(cell.cell_contents for cell in (value.__closure__ or ()))
            == tuple(cell.cell_contents for cell in _AUTOESCAPE.__closure__))


def _compatible(environment: Environment) -> bool:
    return (
        type(environment) is Environment
        and _default_autoescape(environment.autoescape)
        and environment.optimized
        and not environment.is_async
        and not environment.extensions
        and not environment.sandboxed
        and environment.finalize is None
        and environment.undefined is Undefined
        and environment.code_generator_class is CodeGenerator
        and environment.block_start_string == '{%'
        and environment.block_end_string == '%}'
        and environment.variable_start_string == '{{'
        and environment.variable_end_string == '}}'
        and environment.comment_start_string == '{#'
        and environment.comment_end_string == '#}'
        and environment.line_statement_prefix is None
        and environment.line_comment_prefix is None
        and not environment.trim_blocks
        and not environment.lstrip_blocks
        and environment.newline_sequence == '\n'
        and not environment.keep_trailing_newline
        and environment.filters == DEFAULT_FILTERS
        and environment.tests == DEFAULT_TESTS
        and environment.policies == _POLICIES
    )


class PackagedTemplateCache(BytecodeCache):
    def __init__(self, path: Path) -> None:
        self.entries: dict[str, str] = {}
        try:
            manifest = json.loads(path.read_text())
            if (isinstance(manifest, dict) and manifest.get('version') == 1
                    and manifest.get('python') == sys.implementation.cache_tag
                    and manifest.get('jinja') == jinja2.__version__
                    and isinstance(manifest.get('entries'), dict)):
                self.entries = manifest['entries']
        except (OSError, ValueError, TypeError):
            pass  # Source installs and incomplete artifacts use Jinja's compiler.

    def get_cache_key(self, name, filename=None):
        # Installation paths differ from build paths. Jinja's separate source
        # checksum still rejects changed templates and same-name plugin sources.
        return hashlib.sha256(name.encode()).hexdigest()

    def load_bytecode(self, bucket):
        if not _compatible(bucket.environment):
            return
        encoded = self.entries.get(bucket.key)
        if not isinstance(encoded, str):
            return
        try:
            bucket.bytecode_from_string(base64.b64decode(encoded, validate=True))
            if bucket.code is not None and not isinstance(bucket.code, CodeType):
                bucket.reset()
        except (ValueError, TypeError, EOFError, pickle.UnpicklingError):
            bucket.reset()

    def dump_bytecode(self, bucket):
        pass  # The immutable package is never a runtime write destination.


def build_template_cache(template_root: Path, output: Path) -> None:
    """Compile package templates during wheel construction, never service boot."""
    cache = PackagedTemplateCache.__new__(PackagedTemplateCache)
    cache.entries = {}

    def capture(bucket):
        cache.entries[bucket.key] = base64.b64encode(bucket.bytecode_to_string()).decode('ascii')

    cache.dump_bytecode = capture
    environment = Environment(loader=FileSystemLoader(template_root), autoescape=select_autoescape(),
                              bytecode_cache=cache, cache_size=0)
    compile_source = environment.compile

    def compile(source, name=None, filename=None, raw=False, defer_init=False):
        # Build roots are temporary and host-specific. Keep tracebacks useful
        # without baking those paths into the immutable wheel's bytecode.
        return compile_source(source, name=name, filename=f'pa/server/templates/{name}',
                              raw=raw, defer_init=defer_init)

    environment.compile = compile
    for name in sorted(environment.list_templates()):
        environment.get_template(name)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({'version': 1, 'python': sys.implementation.cache_tag,
                                 'jinja': jinja2.__version__, 'entries': cache.entries},
                                sort_keys=True, separators=(',', ':'))+'\n')
