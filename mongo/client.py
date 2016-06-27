class client:

  def __init__(self, ip, port):

    if '' == ip:
        print("Ip can't be empty")
        sys.exit(2)
    self.ip = ip

    if '' == port or 0 == port:
        print("Port is invalid")
        sys.exit(2)
    self.port = port

    from pymongo import MongoClient
    try:
        mongoclient = MongoClient('mongodb://' + self.ip + ':' + self.port)
        #self.client = mongoclient[self.db]

    except pymongo.errors.ConnnectionFailure, e:
        print('Could not connect to MongoDB: %s' % e)
        sys.exit(2)
    return

  def printSelf(self):
      if mongoclient is None:
          print("MongoDB ip: " + self.ip + "port: " + self.port)
      else
        print("")
