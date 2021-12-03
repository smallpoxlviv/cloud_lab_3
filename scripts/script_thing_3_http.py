import requests
import argparse
import time


cert_path = "./aws_keys/0ff331861ec522becc8547c88dcbc6f47f0747a12f51fd0b00319ac9d4322c58-certificate.pem.crt"
key_path = "./aws_keys/0ff331861ec522becc8547c88dcbc6f47f0747a12f51fd0b00319ac9d4322c58-private.pem.key"
endpoint = "ahdkfnapoyirm-ats.iot.eu-central-1.amazonaws.com"
topic = "$aws/things/cloud_lab_3_thing_3/shadow/update"
publish_url = 'https://' + endpoint + ':8443/topics/' + topic + '?qos=1'

sensor_id = 3
name = "name_3"
settlement = "settlement_3"
latitude = "13.1234567"
longtitude = "13.1234567"
water_level = 13

counter = 0
while True:
        counter+=1
        if counter >= 200:
                counter = 0
                x = random.uniform(0,1)
                if x > 0.5:
                        water_level+=1
                else:
                        water_level-=1
                if water_level <= 0 or water_level >= 20:
                        water_level = 13
                
        # Shadow JSON Message formware
        messageJson = '{"state":{"reported":{"sensor_id": ' + str(sensor_id) + ',"name":"' + name + '","settlement": "' + settlement + '" ,"latitude": "' + latitude + '","longtitude": "' + longtitude + '" ,"water_level": ' + str(water_level) + '}}}'
        publish = requests.request('POST',publish_url ,data=messageJson,cert=[cert_path, key_path])

        # print results
        print("Response status: ", str(publish.status_code))
        if publish.status_code == 200:
                print("Response body:", publish.text)
        time.sleep(0.09)