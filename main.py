from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
import ssl

app = Flask(__name__)

MQTT_BROKER = "d4abf07c55364762bf6b41af1122a4ab.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "johan"
MQTT_PASSWORD = "Johan123."

@app.route('/publicar_comando', methods=['POST'])
def publicar_comando():
    data = request.get_json()
    lote = data.get("lote")
    comando = data.get("comando")
    angulo = data.get("angulo")
    caudal = data.get("caudal")

    if not lote or not comando:
        return jsonify({"error": "Campos 'lote' y 'comando' son requeridos"}), 400

    topic = f"caudal/lote/{lote}/comandos"
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
        client.publish(topic, str(payload))
        client.loop_stop()
        client.disconnect()
        return jsonify({"mensaje": "Comando publicado con éxito"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
