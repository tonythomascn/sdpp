class db:
    def __init__(self, client, database):
        if client is None:
            print("MongoDB client is null")
            sys.exit(2)
        self.client = client

        if '' == database:
            print("MongoDB database can't be empty")
            sys.exit(2)
        self.db = database
        self.client
        return

    def put():
        return
    def get():
        return
