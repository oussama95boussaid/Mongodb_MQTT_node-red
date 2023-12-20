from pymongo import MongoClient
import sys
import re
import paho.mqtt.client as paho
import datetime



# Connect to MongoDB
Mongodb_client = MongoClient("mongodb+srv://root:root@clusteriot.qtq6au5.mongodb.net/?retryWrites=true&w=majority")
db = Mongodb_client['Lab5_db']
Home = db['Home']
    

# function for handling the message come from MQTT broker (nod-red)
def message_handling(client, userdata, msg):
    temp = False
    hum = False
    gas = False
    receiveTime=datetime.datetime.now()
    data={"Time":receiveTime,"topic":msg.topic,"value":msg.payload.decode("utf-8")}
    
    if data["topic"].find('temp') > 0:
        temp = True
    if data["topic"].find('hum') > 0:
        hum = True
    if data["topic"].find('gas') > 0:
        gas = True
    
    if temp and int(msg.payload.decode("utf-8")) > 30:
        
        if db.Home.find_one({"value":data["value"]}) :
            print("Temp Value : "+msg.payload.decode()+" already exist!! in Temp")
        else:
            print(f"{msg.topic}: {msg.payload.decode()}")
            db.Home.insert_one(data)
            
    if hum and int(msg.payload.decode("utf-8")) > 50:
        
        if db.Home.find_one({"value":data["value"]}) :
            print("Hum Value : "+msg.payload.decode()+" already exist!! in Hum")
        else:
            print(f"{msg.topic}: {msg.payload.decode()}")
            db.Home.insert_one(data)
        
    if gas and int(msg.payload.decode("utf-8")) > 300:
        
        if db.Home.find_one({"value":data["value"]}) :
            print("Gas Value : "+msg.payload.decode()+" already exist!! in Gas")
        else:
            print(f"{msg.topic}: {msg.payload.decode()}")
            db.Home.insert_one(data)
        

    




# Connect to MQTT broker
MQTT_client = paho.Client()
MQTT_client.on_message = message_handling


# Test Connection to MQTT broker
if MQTT_client.connect("127.0.0.1", 1883) != 0:
    print("Couldn't connect to the mqtt broker")
    sys.exit(1)
else:
    print("Connected to MQTT broker")

# subscribe to the topics
MQTT_client.subscribe("Home/Bedroom/temp")
MQTT_client.subscribe("Home/Bedroom/hum")
MQTT_client.subscribe("Home/Kitchen/temp")
MQTT_client.subscribe("Home/Kitchen/hum")
MQTT_client.subscribe("Home/Kitchen/gas") 
MQTT_client.subscribe("Home/Restroom/temp")
MQTT_client.subscribe("Home/Restroom/hum")




# Disconnect from the MQTT broker
try:
    print("Press CTRL+C to exit...")
    MQTT_client.loop_forever()
except Exception:
    print("Caught an Exception, something went wrong...")
finally:
    print("Disconnecting from the MQTT broker")
    MQTT_client.disconnect()

