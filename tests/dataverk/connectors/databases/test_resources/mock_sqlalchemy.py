class EngineMock:
    def execute(self, sql: str):
        pass

    def dispose(self):
        pass


def mock_create_engine_execute(db: str):
    return EngineMock()
