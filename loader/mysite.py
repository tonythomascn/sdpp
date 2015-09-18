import antelope._datascope as _datascope
from pymongo import errors
 
"""
'site'
#'sta', 'ondate', 'offdate', 'lat', 'lon', 'elev', 'staname', 'statype', 'refsta'
#'dnorth', 'deast', 'lddate'
"""

def load(antedbptr, mongodb):
    try:
#        mongodb.createCollection('site')
        collection = mongodb['site']
    except errors.CollectionInvalid, e:
        print('Collection %s is not valid' % e)
        return
    
    try:
        for antedbptr.record in range(antedbptr.query("dbRECORD_COUNT")):
            sta, ondate, offdate, lat, lon, elev, staname, statype, refsta, dnorth, deast, lddate = antedbptr.getv('sta', 'ondate', 'offdate', 'lat', 'lon', 'elev', 'staname', 'statype', 'refsta', 'dnorth', 'deast', 'lddate')
        #print sta, lat, lon, elev
            collection.save({'sta' : sta,
                            'ondate' : ondate,
                            'offdate' : offdate,
                            'lat' : lat,
                            'lon' : lon,
                            'elev' : elev,
                            'staname' : staname,
                            'statype' : statype,
                            'refsta' : refsta,
                            'dnorth' : dnorth,
                            'deast' : deast,
                            'lddate' : lddate})
    except errors.ExecutionTimeout, e:
        print('Operation execution timeout %s' % e)
        return
