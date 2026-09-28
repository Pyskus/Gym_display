import json
import os
import subprocess
from flask import jsonify, request
from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit
import time
import threading
import asyncio
from bleak import BleakClient


app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

TEMPLATES_FILE = "templates.json"


def load_templates():
    """Charge les modèles depuis le fichier JSON."""
    if not os.path.exists(TEMPLATES_FILE):
        return {}
    try:
        with open(TEMPLATES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Erreur lors de la lecture de {TEMPLATES_FILE} : {e}")
        return {}


def save_templates(templates_data):
    """Sauvegarde les modèles dans le fichier JSON."""
    try:
        with open(TEMPLATES_FILE, "w", encoding="utf-8") as f:
            json.dump(templates_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erreur lors de la sauvegarde de {TEMPLATES_FILE} : {e}")

SENSORS_FILE = "sensors.json"

def load_sensors():
    if os.path.exists(SENSORS_FILE):
        try:
            with open(SENSORS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_sensors(sensors):
    with open(SENSORS_FILE, "w", encoding="utf-8") as f:
        json.dump(sensors, f, indent=4, ensure_ascii=False)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/coach')
def coach():
    return render_template('coach.html', state=timer_state)

@app.route('/config')
def config():
    return render_template('config.html')

@socketio.on("get_templates")
def handle_get_templates():
    """Envoie la liste des modèles au client qui la demande."""
    templates = load_templates()
    emit("update_templates", templates)


@socketio.on("save_template")
def handle_save_template(data):
    """Enregistre un nouveau modèle et informe tous les clients."""
    name = data.get("name")
    blocks = data.get("blocks")

    if not name or not blocks:
        return

    templates = load_templates()
    templates[name] = {"name": name, "blocks": blocks}
    save_templates(templates)

    # Diffuse la liste mise à jour à toutes les interfaces connectées
    emit("update_templates", templates, broadcast=True)

@socketio.on("load_template")
def handle_load_template(data):
    """Envoie les détails du modèle sélectionné au client."""
    name = data.get("name")
    if not name:
        return

    templates = load_templates()
    if name in templates:
        emit("template_loaded", templates[name])

@socketio.on("delete_template")
def handle_delete_template(data):
    """Supprime un modèle existant."""
    name = data.get("name")
    if not name:
        return

    templates = load_templates()
    if name in templates:
        del templates[name]
        save_templates(templates)

        emit("update_templates", templates, broadcast=True)

@socketio.on("update_colors")
def handle_update_colors(data):
    with thread_lock:
        if "bg_color" in data:
            timer_state["bg_color"] = data["bg_color"]
        if "clock_color" in data:
            timer_state["clock_color"] = data["clock_color"]
        if "logo_opacity" in data:
            timer_state["logo_opacity"] = data["logo_opacity"]

    # Envoie l'événement spécifique de couleur ET la mise à jour globale du timer
    emit("update_colors", data, broadcast=True)
    emit("update_timer", timer_state, broadcast=True)

# --- 2. ÉVÉNEMENTS SOCKETIO POUR LES CAPTEURS ---
@socketio.on('get_sensors')
def handle_get_sensors():
    sensors = load_sensors()
    emit('update_sensors_list', sensors)

@socketio.on('add_sensor')
def handle_add_sensor(data):
    name = data.get('name', '').strip()
    mac = data.get('mac', '').strip().upper()
    if name and mac:
        sensors = load_sensors()
        existing = next((s for s in sensors if s['mac'] == mac), None)
        if existing:
            existing['name'] = name
        else:
            sensors.append({'name': name, 'mac': mac})
        save_sensors(sensors)
        emit('update_sensors_list', sensors, broadcast=True)

@socketio.on('delete_sensor')
def handle_delete_sensor(data):
    mac = data.get('mac', '').strip().upper()
    sensors = load_sensors()
    sensors = [s for s in sensors if s['mac'] != mac]
    save_sensors(sensors)
    emit('update_sensors_list', sensors, broadcast=True)        

timer_state = {
    "mode": "clock",
    "time_left": 0,
    "total_time": 0,
    "phase_name": "",
    "current_phase_color": "#ffffff",
    "bg_color": "#0b0f19",
    "clock_color": "#f3f4f6",
    "logo_opacity": 0.2,
    "running": False,
    "blocks": [
        {
            "repeats_total": 1,
            "repeats_left": 1,
            "phases": [
                {"name": "GO !", "duration": 30, "color": "#22c55e"},
                {"name": "Repos", "duration": 30, "color": "#ef4444"}
            ]
        }
    ], 
    "current_block_index": 0,
    "current_phase_index": 0,
    "participants": [] # Liste des participants actifs [{id, name, age, mac, hr, max_hr, zone_color}]
}


thread = None
thread_lock = threading.Lock()

HR_MEASUREMENT_UUID = "00002a37-0000-1000-8000-00805f9b34fb"

def get_zone_color(hr, max_hr):
    """Calcule la couleur de zone en fonction du % de la FCMax"""
    if not max_hr or max_hr <= 0: return "#4b5563"
    pct = (hr / max_hr) * 100
    if pct < 60: return "#2563eb"   # Bleu (Repos / Échauffement)
    elif pct < 70: return "#16a34a" # Vert (Brûle-graisse)
    elif pct < 80: return "#f97316" # Jaune (Aérobie)
    elif pct < 90: return "#dc2626" # Rouge (Seuil anaérobie)
    else: return "#9333ea"          # Violet (Max / Alerte)


# Verrou pour s'assurer qu'un SEUL capteur initie sa connexion à la fois
ble_connect_lock = asyncio.Lock()

async def handle_single_sensor(mac, p_id, active_macs):
    """Gère un capteur avec fermeture propre des sockets BlueZ lors des déconnexions"""
    client = None
    try:
        # Séquencement des connexions
        async with ble_connect_lock:
            await asyncio.sleep(0.5)
            client = BleakClient(mac, timeout=8.0)
            await client.connect()

        print(f"[BLE] Capteur {mac} connecté pour le participant {p_id}")

        def handle_data(sender, data):
            flags = data[0]
            is_16bit = flags & 0x01
            bpm = int.from_bytes(data[1:3], byteorder='little') if is_16bit else data[1]
            
            with thread_lock:
                for part in timer_state.get("participants", []):
                    if part["id"] == p_id:
                        part["hr"] = bpm
                        max_h = 220 - part.get("age", 30)
                        part["max_hr"] = max_h
                        part["zone_color"] = get_zone_color(bpm, max_h)
                        break
            
            socketio.emit('update_timer', timer_state)

        await client.start_notify(HR_MEASUREMENT_UUID, handle_data)
        
        # Boucle de maintien avec détection de suppression du participant
        while client.is_connected:
            with thread_lock:
                still_active = any(p.get("id") == p_id for p in timer_state.get("participants", []))
            
            if not still_active:
                print(f"[BLE] Participant {p_id} supprimé. Fermeture du capteur {mac}...")
                break

            await asyncio.sleep(1)

    except Exception as e:
        print(f"[BLE] Extinction ou perte de signal du capteur {mac} : {e}")

    finally:
        # Purge explicite des connexions fantômes (BlueZ)
        if client:
            try:
                if client.is_connected:
                    await client.stop_notify(HR_MEASUREMENT_UUID)
                    await client.disconnect()
            except Exception:
                pass
                
        await asyncio.sleep(3.0)
        active_macs.discard(mac)
        print(f"[BLE] Sockets du capteur {mac} libérés.")


async def ble_reader_task():
    """Surveille les participants et lance les connexions en tâche de fond"""
    active_macs = set()
    
    while True:
        try:
            with thread_lock:
                participants = list(timer_state.get("participants", []))
            
            for p in participants:
                mac = p.get("mac")
                p_id = p.get("id")
                
                if mac and mac not in active_macs:
                    active_macs.add(mac)
                    asyncio.create_task(handle_single_sensor(mac, p_id, active_macs))

        except Exception as e:
            print(f"[BLE] Erreur boucle principale : {e}")

        await asyncio.sleep(2.0)


def start_ble_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(ble_reader_task())

# Lancement dans un thread séparé
threading.Thread(target=start_ble_loop, daemon=True).start()

def load_current_phase():
    global timer_state
    b = timer_state["blocks"][timer_state["current_block_index"]]
    p = b["phases"][timer_state["current_phase_index"]]
    timer_state["phase_name"] = p["name"]
    timer_state["time_left"] = int(p["duration"])
    timer_state["total_time"] = int(p["duration"])
    timer_state["current_phase_color"] = p["color"]

def go_next_phase():
    global timer_state
    b_idx = timer_state["current_block_index"]
    p_idx = timer_state["current_phase_index"]
    current_block = timer_state["blocks"][b_idx]
    
    p_idx += 1
    if p_idx < len(current_block["phases"]):
        timer_state["current_phase_index"] = p_idx
        load_current_phase()
    else:
        current_block["repeats_left"] -= 1
        if current_block["repeats_left"] > 0:
            timer_state["current_phase_index"] = 0
            load_current_phase()
        else:
            b_idx += 1
            if b_idx < len(timer_state["blocks"]):
                timer_state["current_block_index"] = b_idx
                timer_state["current_phase_index"] = 0
                load_current_phase()
            else:
                timer_state["running"] = False
                timer_state["phase_name"] = "Terminé !"
                timer_state["current_phase_color"] = timer_state["bg_color"]
                socketio.emit('play_beep', {'type': 'end'})

def go_prev_phase():
    global timer_state
    b_idx = timer_state["current_block_index"]
    p_idx = timer_state["current_phase_index"]
    current_block = timer_state["blocks"][b_idx]
    
    p_idx -= 1
    if p_idx >= 0:
        timer_state["current_phase_index"] = p_idx
        load_current_phase()
    else:
        if current_block["repeats_left"] < current_block["repeats_total"]:
            current_block["repeats_left"] += 1
            timer_state["current_phase_index"] = len(current_block["phases"]) - 1
            load_current_phase()
        else:
            b_idx -= 1
            if b_idx >= 0:
                timer_state["current_block_index"] = b_idx
                prev_block = timer_state["blocks"][b_idx]
                prev_block["repeats_left"] = 1
                timer_state["current_phase_index"] = len(prev_block["phases"]) - 1
                load_current_phase()
            else:
                timer_state["current_block_index"] = 0
                timer_state["current_phase_index"] = 0
                load_current_phase()

def timer_background_task():
    global timer_state
    while True:
        socketio.sleep(1)
        with thread_lock:
            if timer_state["running"]:
                if timer_state["time_left"] > 0:
                    timer_state["time_left"] -= 1
                    if 0 < timer_state["time_left"] <= 3:
                        socketio.emit('play_beep', {'type': 'normal'})
                    elif timer_state["time_left"] == 0:
                        socketio.emit('play_beep', {'type': 'long'})
                    socketio.emit('update_timer', timer_state)
                else:
                    go_next_phase()
                    socketio.emit('update_timer', timer_state)


@socketio.on('connect')
def handle_connect():
    emit('update_timer', timer_state)


@socketio.on('update_participants')
def handle_update_participants(data):
    with thread_lock:
        timer_state["participants"] = data.get('participants', [])
    emit('update_timer', timer_state, broadcast=True)

@socketio.on('start_program')
def handle_start_program(data):
    global thread, timer_state
    with thread_lock:
        timer_state["blocks"] = data['blocks']
        timer_state["mode"] = "timer"
        timer_state["current_block_index"] = 0
        timer_state["current_phase_index"] = 0
        
        if len(timer_state["blocks"]) > 0:
            load_current_phase()
            timer_state["running"] = True
            
        if thread is None:
            thread = socketio.start_background_task(target=timer_background_task)
            
    socketio.emit('play_beep', {'type': 'long'})
    emit('update_timer', timer_state, broadcast=True)

@socketio.on('next_phase')
def handle_next_phase():
    with thread_lock: go_next_phase()
    emit('update_timer', timer_state, broadcast=True)

@socketio.on('prev_phase')
def handle_prev_phase():
    with thread_lock: go_prev_phase()
    emit('update_timer', timer_state, broadcast=True)

@socketio.on('pause_timer')
def handle_pause():
    with thread_lock: timer_state["running"] = False
    emit('update_timer', timer_state, broadcast=True)

@socketio.on('resume_timer')
def handle_resume():
    with thread_lock: timer_state["running"] = True
    emit('update_timer', timer_state, broadcast=True)

@socketio.on('stop_timer')
def handle_stop():
    with thread_lock:
        timer_state["running"] = False
        timer_state["mode"] = "clock"
    emit('update_timer', timer_state, broadcast=True)

@socketio.on('get_sensors')
def handle_get_sensors():
    sensors = load_sensors()
    emit('update_sensors_list', sensors)

@socketio.on('add_sensor')
def handle_add_sensor(data):
    name = data.get('name', '').strip()
    mac = data.get('mac', '').strip().upper()
    
    if name and mac:
        sensors = load_sensors()
        existing = next((s for s in sensors if s['mac'] == mac), None)
        if existing:
            existing['name'] = name
        else:
            sensors.append({'name': name, 'mac': mac})
        
        save_sensors(sensors)
        emit('update_sensors_list', sensors, broadcast=True)

@socketio.on('delete_sensor')
def handle_delete_sensor(data):
    mac = data.get('mac', '').strip().upper()
    sensors = load_sensors()
    sensors = [s for s in sensors if s['mac'] != mac]
    save_sensors(sensors)
    emit('update_sensors_list', sensors, broadcast=True)

@app.route('/api/reboot', methods=['POST'])
def reboot_pi():
    try:
        # Lance le redémarrage après un court délai pour permettre l'envoi de la réponse au téléphone
        subprocess.Popen(["sudo", "reboot"])
        return jsonify({"status": "ok", "message": "Redémarrage en cours..."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    
from flask import send_from_directory

@app.route('/manifest.json')
def serve_manifest():
    return send_from_directory('static', 'manifest.json')

@app.route('/sw.js')
def serve_sw():
    return send_from_directory('static', 'sw.js', mimetype='application/javascript')

if __name__ == '__main__':
    # Lancement du thread Bluetooth en arrière-plan
    threading.Thread(target=start_ble_loop, daemon=True).start()
    socketio.run(
        app,
        host='0.0.0.0',
        port=5000,
        allow_unsafe_werkzeug=True,
        debug=False,
        ssl_context=(
            '/etc/letsencrypt/live/dommatimer.duckdns.org/fullchain.pem',
            '/etc/letsencrypt/live/dommatimer.duckdns.org/privkey.pem',
        ),
    )