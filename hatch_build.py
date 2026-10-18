"""Compile wheel-owned templates without booting PA or writing service state."""
from pathlib import Path
import runpy
import tempfile

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        if self.target_name != 'wheel':
            return
        self._template_directory = tempfile.TemporaryDirectory(prefix='pa-wheel-templates-')
        package = Path(self.root) / 'src' / 'pa'
        output = Path(self._template_directory.name) / 'template-bytecode.json'
        builder = runpy.run_path(str(package / 'server' / 'template_cache.py'))
        builder['build_template_cache'](package / 'server' / 'templates', output)
        build_data['force_include'][str(output)] = 'pa/server/template-bytecode.json'

    def finalize(self, version, build_data, artifact_path):
        directory = getattr(self, '_template_directory', None)
        if directory is not None:
            directory.cleanup()
