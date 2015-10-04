import sys
import os

"""
Data loader main entrance, accepting arguments and loading specified table loader.
"""
def usage():
    print 'python transfer.py [OPTIONS]\n\
    \t -h --help\t\t\tPrint this help screen\n\
    \t -i --mongodb_host\t\tMongoDB server host (default: localhost)\n\
    \t -p --mongodb_port\t\tMongoDB server port (default: 27017)\n\
    \t -w --waveform_path\t\tWaveform absolute path\n'

def createMongoClient(mongodb_host, mongodb_port):
    from pymongo import MongoClient
    try:
        mongoclient = MongoClient('mongodb://' + mongodb_host + ':' + mongodb_port)
        mongodb = mongoclient['test']
        return mongodb
    except pymongo.errors.ConnnectionFailure, e:
        print('Could not connect to MongoDB: %s' % e)
        sys.exit(2)

def main(argv):
    import getopt
    try:
        opts, args = getopt.getopt(argv, "w:iph", ["waveform_path=", \
        "mongodb_host=", "mongodb_port=", "help"])
        if not opts:
            usage()
            sys.exit(2)
        mongodb_host = '127.0.0.1'
        mongodb_port = '27017'
        waveform_path = ''
        for opt, arg in opts:
            if opt in ('-w', '--waveform_path'):
                waveform_path = arg
            elif opt in ('-i', '--mongodb_host'):
                mongodb_host = arg
            elif opt in ('-p', '--mongodb_port'):
                mongodb_port = arg
            elif opt in ('-h', '--help'):
                usage()
                sys.exit(2)

    except getopt.GetoptError:
        usage()
        sys.exit(2)
    if '' == waveform_path:
        usage()
        sys.exit(2)
    
    #init mongodb collection
    mongodb = createMongoClient(mongodb_host, mongodb_port)
    try:
        collection = mongodb['wfdisc']
    except errors.CollectionInvalid, e:
        print('Collection %s is not valid' % e)
        return


    #create gridfs file descripter
    import gridfs
    fs = gridfs.GridFS(mongodb)

    cursor = collection.find()
    count = 0
    for wf in cursor:
        name = waveform_path + '/' + wf['dir'] + '/' + wf['dfile']
        #print(name)
        with fs.new_file(filename=wf['dir'] + '/' + wf['dfile'], content_type='chunks') as fp:
            file = open(name, 'r')
            fp.write(file.read())
            file.close()
            count += 1
    print('%d files have been transfered into GridFS' % count)

    
if __name__ == "__main__":
    main(sys.argv[1:])
