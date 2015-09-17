import sys
import os
sys.path.append( os.environ['ANTELOPE'] + '/data/python' )
#add the current path to python path, so user module can be found
sys.path.append('./')
from antelope.datascope import *
"""
Data loader main entrance, accepting arguments and loading specified table loader.
"""
def usage():
    print 'python loader.py [OPTIONS]\n\
    \t -h --help\t\t\tPrint this help screen\n\
    \t -d --antelope_db\t\tAntelope database directory (mandatory)\n\
    \t -t --antelope_table\t\tAntelope table (mandatory)\n\
    \t -i --mongodb_host\t\tMongoDB server host (default: localhost)\n\
    \t -p --mongodb_port\t\tMongoDB server port (default: 28017)\n\
    \t -u --mongodb_userid\t\tMongoDB user id\n\
    \t -w --mongodb_pwd\t\tMongoDB user password'

def createMongoClient(mongodb_host, mongodb_port):
    from pymongo import MongoClient
    try:
        mongoclient = MongoClient('mongodb://' + mongodb_host + ':' + mongodb_port)

        mongodb = mongoclient.test_database
        collection = mongodb.test_collection
        return mongoclient
    except pymongo.errors.ConnnectionFailure, e:
        print('Could not connect to MongoDB: %s' % e)
        sys.exit(2)

def main(argv):
    import getopt
    try:
        opts, args = getopt.getopt(argv, "d:t:ipuwh", ["antelope_db=", \
        "antelope_table=", "mongodb_host=", "mongodb_port=", "mongodb_userid=", "mongodb_pwd=", "help"])
        if not opts:
            usage()
            sys.exit(2)
        antelope_db = ''
        antelope_table = ''
        mongodb_host = ''
        mongodb_port = '28017'
        mongodb_userid = ''
        mongodb_pwd = ''
        for opt, arg in opts:
            if opt in ('-d', '--antelope_db'):
                antelope_db = arg
            elif opt in ('-i', '--mongodb_host'):
                mongodb_host = arg
            elif opt in ('-p', '--mongodb_port'):
                mongodb_port = arg
            elif opt in ('-u', '--mongdb_userid'):
                mongodb_userid = arg
            elif opt in ('-w', '--mongodb_pwd'):
                mongodb_pwd = arg
            elif opt in ('-t', '--antelope_table'):
	        antelope_table = arg
            elif opt in ('-h', '--help'):
                usage()
                sys.exit(2)

    except getopt.GetoptError:
        usage()
        sys.exit(2)
    if '' == antelope_db or '' == antelope_table:
        usage()
        sys.exit(2)
    if antelope_db.endswith('.'):
        antelope_db = antelope_db[:-1]
    
    #init antelope database pointer and mongodb pointer
    from antelope.datascope import dbopen
    #antedbptr = ''
    antedb = dbopen( antelope_db, "r" )
    antedbptr = antedb.lookup(table=antelope_table)
    #dbtable = db.schema_tables['site']
    #for dbrecord in dbtable.iter_record():
    #    print repr(dbrecord)
    
    #after prepare for all the arguments, load the [table].py module
    import importlib
    #the name of 'site' has been taken, so new name is assigned
    if 'site' == antelope_table:
        antelope_table = 'my' + antelope_table
    load_module = __import__(antelope_table)
    getattr(load_module, 'load')(antedbptr, mongodb)
    
    #after loading, close the pointer and connection
    antedb.close()
    

if __name__ == "__main__":
    main(sys.argv[1:])