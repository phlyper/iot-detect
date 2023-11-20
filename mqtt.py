import paho.mqtt.client as paho
from paho import mqtt
from datetime import datetime
import json
import mysql.connector

from config import load_config
config = load_config()

mysql_db = mysql.connector.connect(
  host=config['MYSQL_HOST'],
  port=config['MYSQL_PORT'],
  user=config['MYSQL_USERNAME'],
  password=config['MYSQL_PASSWORD'],
  database=config['MYSQL_DBNAME'],
)

mqtt_topic = "iot"

def on_connect(client, userdata, flags, rc, properties=None):
    print('CONNACK received with code %s.' % (rc))
    
def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: "+str(mid)+" "+str(granted_qos))


def on_message(client, userdata, msg, properties=None):
    print(msg.topic+" "+str(msg.qos)+" "+str(msg.payload), type(msg), type(msg.payload))

    if mysql_db is not None:
        try:

            # Getting the current date and time
            dt = datetime.now()

            payload = msg.payload.decode()
            print("payload", payload)

            if payload:
                payload_data = json.loads(payload)
                print("payload", payload, "payload_data", payload_data, "type payload", type(payload), "dt", dt)

                if payload_data:
                    sql = "INSERT INTO data (message, topic, object, accuracy, qos, detected_at, created_at, updated_at) VALUES (%s, %s, %s, %f, %d, %d, %d, %d)"
                    values = (payload, msg.topic, payload_data['prediction'], payload_data['confidence'], msg.qos, int(payload_data['detected_at']), int(datetime.timestamp(dt)), int(datetime.timestamp(dt)))

                    print("sql", sql, "values", values)

                    mysql_cursor = mysql_db.cursor()
                    mysql_cursor.execute(sql, values)

                    print("1 record inserted, ID:", mysql_cursor.lastrowid) 

                    mysql_db.commit()

        except Exception as e:
            print(f"Error mysql {e}")
        finally:
            mysql_cursor.close()


client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect
# enable TLS for secure connection
client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
# set username and password
client.username_pw_set(config['MQTT_USERNAME'], config['MQTT_PASSWORD'])
# connect to HiveMQ Cloud on port 8883 (default for MQTT)
client.connect(config['MQTT_HOST'], int(config['MQTT_PORT']))

client.on_subscribe = on_subscribe
client.on_message = on_message

# subscribe to all topics of encyclopedia by using the wildcard "#"
client.subscribe(mqtt_topic, qos=0)

# client.loop_start()

# loop_forever for simplicity, here you need to stop the loop manually
# you can also use loop_start and loop_stop
client.loop_forever()

mysql_db.close()
