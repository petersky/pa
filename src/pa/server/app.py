from pa.core.kernel import Kernel


def create_app():
    return Kernel.boot().build_app()
