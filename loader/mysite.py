import sys
import antelope._datascope as _datascope
"""
'site'
#'sta', 'ondate', 'offdate', 'lat', 'lon', 'elev', 'staname', 'statype', 'refsta'
#'dnorth', 'deast', 'lddate'
"""

def load(antedbptr, mongodb):
    for antedbptr.record in range(antedbptr.query("dbRECORD_COUNT")):
        sta, lat, lon, elev = antedbptr.getv('sta', 'lat', 'lon', 'elev')
#        print sta, lat, lon, elev

