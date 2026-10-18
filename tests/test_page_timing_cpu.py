import re
import time

from pa.modules.ui_shell import _Timings


def test_page_timing_retains_wall_phase_and_separately_reports_thread_cpu():
    timings = _Timings()
    token = object()

    def waiting_phase():
        time.sleep(.01)
        return token

    assert timings.measure('template', waiting_phase) is token
    values = {name: float(value) for name, value in
              re.findall(r'([a-z_]+);dur=([0-9.]+)', timings.header())}
    assert {'template', 'total', 'template_cpu', 'total_cpu'} == values.keys()
    assert values['template'] >= 5
    assert values['template_cpu'] < values['template']
    assert values['total'] >= values['template']
    assert values['total_cpu'] >= values['template_cpu']
