from pa.core.kernel import Kernel


def create_app():
    return Kernel.boot(claim_writer=True).build_app()
