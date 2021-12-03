import sys
import logging
import rds_config
import pymysql
import json

#rds settings
rds_host  = "lab2database.chov87qbwh0h.eu-central-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

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
    """
    This function fetches content from MySQL RDS instance
    """
    
    data = []
    with conn.cursor() as cur:
        cur.execute("select * from river;")
        rows = cur.fetchall()
        for row in rows:
            row_dict = {
                'id': row[0],
                'sensor_id': row[1],
                'name': row[2], 
                'settlement': row[3], 
                'latitude': str(row[4]),
                'longtitude': str(row[5]),
                'water_level': row[6],
                'measurement_date': str(row[7])
            }
            data.append(row_dict)
            logger.info(row_dict)
        logger.info(data)
    conn.commit()
    


    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(data)
    }