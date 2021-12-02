import struct
import sys
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import random

######################################################
# Send Data to AWS
######################################################

host = "ahdkfnapoyirm-ats.iot.eu-central-1.amazonaws.com"
rootCAPath = "./aws_keys/AmazonRootCA1.pem"
certificatePath = "./aws_keys/0ff331861ec522becc8547c88dcbc6f47f0747a12f51fd0b00319ac9d4322c58-certificate.pem.crt"
privateKeyPath = "./aws_keys/0ff331861ec522becc8547c88dcbc6f47f0747a12f51fd0b00319ac9d4322c58-private.pem.key"
port = 8883
clientId = "cloud_lab_3_thing_2"
topic = "$aws/things/cloud_lab_3_thing_2/shadow/update"

sensor_id = 2
name = "name_2"
settlement = "settlement_2"
latitude = "12.1234567"
longtitude = "12.1234567"
water_level = 12

myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureEndpoint(host, port)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

myAWSIoTMQTTClient.connect()

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
			water_level = 12
		
	# Shadow JSON Message formware
	messageJson = '{"state":{"reported":{"sensor_id": ' + str(sensor_id) + ',"name":"' + name + '","settlement": "' + settlement + '" ,"latitude": "' + latitude + '","longtitude": "' + longtitude + '" ,"water_level": ' + str(water_level) + '}}}'
	myAWSIoTMQTTClient.publish(topic, messageJson, 1)
	time.sleep(0.08)

