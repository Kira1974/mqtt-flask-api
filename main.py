from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import ssl
import time
import json
import os
import threading
import requests
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

MQTT_BROKER = "d4abf07c55364762bf6b41af1122a4ab.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "johan"
MQTT_PASSWORD = "Johan123."
DJANGO_ENDPOINT = os.getenv(
    "DJANGO_ENDPOINT",
    "http://127.0.0.1:8000/api/esp32/recibir-consumo"
)

# --- MQTT CALLBACK PARA DATOS ---
def on_connect(client, userdata, flags, rc):
    print("✅ Conectado al broker MQTT para recibir datos")
    client.subscribe("caudal/lote/1852896-025/datos")

last_payload = None

def on_message(client, userdata, msg):
    global last_payload
    payload_str = msg.payload.decode()
    if payload_str == last_payload:
        print("Mensaje duplicado ignorado")
        return
    last_payload = payload_str
    print(f"📩 Mensaje recibido en {msg.topic}:", payload_str)
    try:
        payload = json.loads(payload_str)
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

@app.route('/publicar_comando_lote', methods=['POST'])
def publicar_comando_lote():
    data = request.get_json()
    comando = data.get("comando")
    angulo = data.get("angulo")
    caudal = data.get("caudal")
    lote_id = data.get("lote_id")

    if not comando:
        return jsonify({"error": "El campo 'comando' es obligatorio"}), 400
    if not lote_id:
        return jsonify({"error": "El campo 'lote_id' es obligatorio para publicar comando en lote"}), 400

    topic = f"caudal/lote/{lote_id}/comandos"
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
        return jsonify({"mensaje": f"Comando publicado en lote {lote_id} con éxito"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/publicar_comando_bocatoma', methods=['POST'])
def publicar_comando_bocatoma():
    data = request.get_json()
    comando = data.get("comando")
    angulo = data.get("angulo")
    caudal = data.get("caudal")
    id_valvula = data.get("id_valvula")

    if not comando:
        return jsonify({"error": "El campo 'comando' es obligatorio"}), 400
    if not id_valvula:
        return jsonify({"error": "El campo 'id_valvula' es obligatorio para bocatoma"}), 400

    topic = f"caudal/bocatoma/{id_valvula}/comandos"
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
        return jsonify({"mensaje": f"Comando publicado en bocatoma {id_valvula} con éxito"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    threading.Thread(target=start_mqtt_listener, daemon=True).start()
    app.run(host='0.0.0.0', port=8000)
