import paho.mqtt.client as mqtt
import time
import sys
from os import getcwd
import pyodbc

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = 12
MAX_RECONNECT_DELAY = 60

def on_disconnect(client, userdata, rc):
    print("Disconnected with result code: {}".format(rc))
    reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
    while reconnect_count < MAX_RECONNECT_COUNT:
        print("Reconecting in {} seconds...".format(reconnect_delay))
        time.sleep(reconnect_delay)

        try:
            client.reconnect()
            print("Reconnected successfully!")
            return
        except Exception as err:
            print("Reconnect failed: {}".format(err))

        reconnect_delay *= RECONNECT_RATE
        reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
        reconnect_count += 1
    print("Reconnect failed after {} attempts. Exiting...".format(reconnect_count))

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("Inyectoras/#")

def on_message(client, userdata, msg):
    try:
        topic_parts = msg.topic.split('/')

        # expected topic form: Inyectoras/Inyectora_X/<subtopic>
        _, machine, subtopic = topic_parts[:3]
        payload = msg.payload.decode('utf-8', errors='ignore').strip()
        print(f"Received on {machine}/{subtopic}: {payload}")

        # open a fresh DB connection for this message
        conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=C:\Users\FP\Desktop\Bases de Datos\Python\Disponibilidad_Maquinas.accdb;')
        cursor = conn.cursor()

        # simple dispatch by machine and subtopic
        if machine == "Inyectora_1":
            if subtopic == "tiempo_paro":
                cursor.execute('''
                INSERT INTO Inyectora_1 (Tiempo_Paro)
                VALUES (?);
                ''', (payload,))
            else:
                print("Unknown subtopic:", subtopic)
        elif machine == "Inyectora_2":
            if subtopic == "tiempo_paro":
                cursor.execute('''
                INSERT INTO Inyectora_2 (Tiempo_Paro)
                VALUES (?);
                ''', (payload,))
            else:
                print("Unknown subtopic:", subtopic)
        else:
            print("Unhandled machine:", machine)

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("on_message error:", e)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

client.connect("localhost", 1883, 60)

""" cursor.execute('SELECT * FROM Inyectora_1;')
for row in cursor.fetchall():
    print(row) """


client.loop_forever()