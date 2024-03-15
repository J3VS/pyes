from pyes.store import MegaStore


class TestServices:
    def __init__(self, es):
        self.es = es
        self.store = MegaStore(es)
