import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "phase0: Phase 0 — foundation and schema tests")
    config.addinivalue_line("markers", "phase1: Phase 1 — SDK tests")
    config.addinivalue_line("markers", "phase2: Phase 2 — pipeline execution observability")
    config.addinivalue_line("markers", "phase3: Phase 3 — dashboard tests")
    config.addinivalue_line("markers", "phase4: Phase 4 — alerting tests")
    config.addinivalue_line("markers", "phase5: Phase 5 — legacy pipeline support")
    config.addinivalue_line("markers", "phase6: Phase 6 — streaming observability")
    config.addinivalue_line("markers", "phase7: Phase 7 — data quality")
    config.addinivalue_line("markers", "phase8: Phase 8 — SLOs and data contracts")
    config.addinivalue_line("markers", "phase9: Phase 9 — federation and CI gates")
    config.addinivalue_line("markers", "phase10: Phase 10 — self-service onboarding")
    config.addinivalue_line("markers", "phase11: Phase 11 — infrastructure observability")
    config.addinivalue_line("markers", "phase12: Phase 12 — multi-environment and cost controls")
