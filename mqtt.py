from faker import Faker
import paho.mqtt.client as paho
from datetime import datetime
import json
import mysql.connector

mysql_db = mysql.connector.connect(
  host="localhost",
  port="3306",
  user="root",
  password="",
  database="iot"
)

mqtt_topic = "iot"

def on_connect(client, userdata, flags, rc):
    print('CONNACK received with code %d.' % (rc))
    
def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))


def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload))

    if mysql_db is not None:
        try:

            # Getting the current date and time
            dt = datetime.now()

            payload = msg.payload.decode()

            mysql_cursor = mysql_db.cursor()

            sql = "INSERT INTO data (message, topic, object, accuracy, detected_at, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s);"
            values = (msg.payload, mqtt_topic, None, None, datetime.timestamp(dt), datetime.timestamp(dt))
            mycursor.execute(sql, values)

            print("1 record inserted, ID:", mycursor.lastrowid) 

            mydb.commit()

        except Error as e:
            print(f"Error mysql (e)")
        finally:
            mysql_cursor.close()


client = paho.Client()
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message
client.connect('broker.mqttdashboard.com', 1883)
client.loop_start()

mysql_db.close()
