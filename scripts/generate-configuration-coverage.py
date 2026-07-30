"""Print the registry-derived human-readable configuration coverage report."""

from pa.configuration.service import human_coverage_report

print(human_coverage_report(), end="")
