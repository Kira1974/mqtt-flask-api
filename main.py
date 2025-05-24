from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import ssl
import time
import json
import os
import threading
import requests

app = Flask(__name__)

MQTT_BROKER = "d4abf07c55364762bf6b41af1122a4ab.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "johan"
MQTT_PASSWORD = "Johan123."
DJANGO_ENDPOINT = os.getenv(
    "DJANGO_ENDPOINT",
    "https://desarrollo-aquasmart-backend-develop.onrender.com/api/esp32/recibir-consumo"
)
# --- MQTT CALLBACK PARA DATOS ---
def on_connect(client, userdata, flags, rc):
    print("✅ Conectado al broker MQTT para recibir datos")
    client.subscribe("caudal/lote/1852896-025/datos")


def on_message(client, userdata, msg):
    print(f"📩 Mensaje recibido en {msg.topic}:", msg.payload.decode())
    try:
        payload = json.loads(msg.payload.decode())
        requests.post(DJANGO_ENDPOINT, json=payload)
    except Exception as e:
        print("❌ Error al reenviar a Django:", e)


def start_mqtt_listener():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_forever()


@app.route('/publicar_comando', methods=['POST'])
def publicar_comando():
    data = request.get_json()
    comando = data.get("comando")
    angulo = data.get("angulo")
    caudal = data.get("caudal")

    topic = f"caudal/lote/1852896-025/comandos"
    payload = {"comando": comando}
    if angulo is not None:
        payload["angulo"] = angulo
    if caudal is not None:
        payload["caudal"] = caudal

    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.loop_start()
        client.publish(topic, json.dumps(payload))
        time.sleep(1)
        client.loop_stop()
        client.disconnect()
        return jsonify({"mensaje": "Comando publicado con éxito"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    threading.Thread(target=start_mqtt_listener, daemon=True).start()
    app.run(host='0.0.0.0', port=8000)  