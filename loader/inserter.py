import antelope._datascope as _datascope
from pymongo import errors
 
def joinLoad(antedb, mongodb, antelope_table, fields, *join_tables):
    try:
        collection = mongodb[antelope_table]
        print('collection %s will be created' % antelope_table)
    except errors.CollectionInvalid, e:
        print('Collection %s is not valid' % e)
        return

    try:
        print('Collection %s has following fields:' % antelope_table)
        print(fields)
        counter = 0
        for join_table in join_tables:
            joined_view = antedb.join(join_table)
        
        values = []
        chanid = []
        wfid = []
        for joined_view.record in range(joined_view.query('dbRECORD_COUNT')):
            wfid.append(joined_view.getv('wfid'))
            chanid.append(joined_view.getv('chanid'))
            if 3 == len(wfid):
                values = joined_view.getv('sta', 'time', 'arid', 'jdate', 'stassid', 'chan', 'iphase', 'stype', 'deltim', 'azimuth', 'delaz', 'slow', 'delslo', 'ema', 'rect', 'amp', 'per', 'logat', 'clip', 'fm', 'snr', 'qual', 'auth', 'commid', 'lddate')
                dic = dict(zip(fields, values))
                dic['wfid'] = wfid
                dic['chanid'] = chanid
                #print(dic)
                collection.save(dict(zip(fields, values)))
                counter += 1
                chanid = []
                wfid = []
        print('%d records are added to collection %s' % (counter, antelope_table))
    except errors.ExecutionTimeout, e:
        print('Operation execution timeout %s' % e)
        return

def load(antedbptr, mongodb, antelope_table, fields):
    try:
        #module name 'site' has been taken
        if 'mysite' == antelope_table:
            antelope_table = 'site'
        collection = mongodb[antelope_table]
        print('collection %s will be created' % antelope_table)
    except errors.CollectionInvalid, e:
        print('Collection %s is not valid' % e)
        return
    
    try:
        print('Collection %s has following fields:' % antelope_table)
        print(fields)
        counter = 0
        for antedbptr.record in range(antedbptr.query("dbRECORD_COUNT")):
            result = antedbptr.get()
            values = filter(None, [x.strip('\t\n\r') for x in result.split(' ')])
            collection.save(dict(zip(fields, values)))
            counter += 1
        print('%d records are added to collection %s' % (counter, antelope_table))
    except errors.ExecutionTimeout, e:
        print('Operation execution timeout %s' % e)
        return
