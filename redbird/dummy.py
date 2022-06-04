
class DummySession:
    """Dummy session

    Imitates similar session objects as SQLAlchemy's
    session in order to avoid code changes if
    in-memory repository is used.
    """

    def close(self):
        "Close the connection(s)/client(s)/engine(s)"
        ...
    
    def remove(self):
        "Close the connection(s)/client(s)/engine(s) so that they can be recreated"
        ...

    def get_bind(self, bind_key=None):
        "Get connection/client/engine for given key"
        ...
