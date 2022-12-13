#influxdb_client client must be installed in the machine
import influxdb_client
import json
from influxdb_client.client.write_api import SYNCHRONOUS
from aemet import Aemet, Estacion
from datetime import datetime

#Define a few variables with the name of your bucket, organization, and token
bucket = "meteo"
org = "fptxurdinaga"
token = "_8pYVelVbeJwXEdxX3b0Vm46mp0DP1r0i-QN4mBJQdmzU1Z9vuFacLG4C7aXuQiR-hRYFvPKKj0z2JX0T-d7ow=="
# Store the URL of your InfluxDB instance
url="http://192.168.1.24:8086"

client = influxdb_client.InfluxDBClient(
   url=url,
   token=token,
   org=org
)
write_api = client.write_api(write_options=SYNCHRONOUS)


#Get data from Aemet
years = range(2019,2021)
aemet = Aemet(api_key='eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhc2llci5hQGZwdHh1cmRpbmFnYS5jb20iLCJqdGkiOiJhOTFkZWM0Yi0yMTI5LTRkOGYtOTQ2Yy1iNjA1ZjdmYjhlZTMiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTY3MDU3NDc5NCwidXNlcklkIjoiYTkxZGVjNGItMjEyOS00ZDhmLTk0NmMtYjYwNWY3ZmI4ZWUzIiwicm9sZSI6IiJ9.kxiIR046R5i6GDZOd_Qj5uZG4xpVFLPnlqJWPuuT3PI')
estaciones = Estacion.get_estaciones(api_key='eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhc2llci5hQGZwdHh1cmRpbmFnYS5jb20iLCJqdGkiOiJhOTFkZWM0Yi0yMTI5LTRkOGYtOTQ2Yy1iNjA1ZjdmYjhlZTMiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTY3MDU3NDc5NCwidXNlcklkIjoiYTkxZGVjNGItMjEyOS00ZDhmLTk0NmMtYjYwNWY3ZmI4ZWUzIiwicm9sZSI6IiJ9.kxiIR046R5i6GDZOd_Qj5uZG4xpVFLPnlqJWPuuT3PI')
estaciones_bizkaia = [x for x in estaciones if x['provincia'] == 'BIZKAIA']
for e in estaciones_bizkaia:
    for a in years:
        vc = aemet.get_valores_climatologicos_mensuales(a, e['indicativo'])
        for m in vc:
            if "-13" not in m['fecha']:
                if 'tm_mes' in m:
                    fecha = int(datetime.strptime(m['fecha'],'%Y-%m').timestamp())
                    p = influxdb_client.Point("aemet_measurement").tag("location", e['nombre']).field("tm_mes",m['tm_mes'] ).time(fecha)
                    write_api.write(bucket=bucket, org=org, record=p)




#p = influxdb_client.Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
#write_api.write(bucket=bucket, org=org, record=p)





#Query
query = 'from(bucket:"meteo")\
|> range(start: -10y)'
result = query_api.query(org=org, query=query)
