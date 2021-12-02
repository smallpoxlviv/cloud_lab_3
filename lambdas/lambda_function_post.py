import sys
import logging
import rds_config
import pymysql
import json
from datetime import datetime
from pytz import timezone, common_timezones
import pytz

#rds settings
rds_host  = "lab2database.chov87qbwh0h.eu-central-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

state_key ="state"
reported_key = "reported"
    
sensor_id_key = "sensor_id"
name_key = 'name'
settlement_key = 'settlement'
latitude_key = "latitude"
longtitude_key = "longtitude"
water_level_key = "water_level"
time_stamp_key = "timestamp"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
def lambda_handler(event, context):

    if state_key in event:
        if reported_key in event[state_key]:
            if sensor_id_key in event[state_key][reported_key]:
                sensor_id = event[state_key][reported_key][sensor_id_key]
            else:
                raise Exception("JSON Missing sensor_id")
            if name_key in event[state_key][reported_key]:
                name = event[state_key][reported_key][name_key]
            else:
                name = "not received"
            if settlement_key in event[state_key][reported_key]:
                settlement = event[state_key][reported_key][settlement_key]
            else:
                settlement = "not received" 
            if latitude_key in event[state_key][reported_key]:
                latitude = event[state_key][reported_key][latitude_key]
            else:
                latitude = "00.000000" 
            if longtitude_key in event[state_key][reported_key]:
                longtitude = event[state_key][reported_key][longtitude_key]
            else:
                longtitude = "00.000000"  
            if water_level_key in event[state_key][reported_key]:
                water_level = event[state_key][reported_key][water_level_key]
            else:
                water_level = "0" 
            if time_stamp_key in event[state_key][reported_key]:
                time = datetime.fromtimestamp(event[state_key][reported_key][time_stamp_key],tz=pytz.utc)
            elif time_stamp_key in event:
                time = datetime.fromtimestamp(event[time_stamp_key],tz=pytz.utc)
            else:
                raise Exception("JSON Missing time date")

    tz1 = pytz.timezone('US/Eastern')
    xc = time.astimezone(tz1)
    measurement_date = xc.strftime("%Y-%m-%d %H:%M:%S")

    data = [{
        "sensor_id": sensor_id,
        "name": name,
        "settlement": settlement,
        "latitude": latitude,
        "longtitude": longtitude,
        "water_level": water_level,
        "measurement_date": measurement_date
    }]
    
    
    with conn.cursor() as cur:
        sql = "UPDATE `river` SET name=%s, settlement=%s, latitude=%s, longtitude=%s,\
         water_level=%s, measurement_date=%s WHERE sensor_id=%s LIMIT 1;"
        cur.execute(sql, (name, settlement, latitude, longtitude, water_level, measurement_date, sensor_id))
        logger.info(data)
    conn.commit()
    
    
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(data)
    }