from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from kafka import KafkaProducer
import json
import os

# -----------------------------------
# Kafka Setup (Safe for Render)
# -----------------------------------
producer = None
try:
    kafka_server = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"✅ Connected to Kafka at {kafka_server}")
except Exception as e:
    print(f"⚠️ Kafka not available: {e}")

# -----------------------------------
# Flask App
# -----------------------------------
app = Flask(__name__)
CORS(app)

# -----------------------------------
# PostgreSQL Connection
# -----------------------------------
try:
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        database=os.environ.get("DB_NAME", "mod_system"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "twitter77")
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL successfully")
except Exception as e:
    print(f"❌ PostgreSQL connection failed: {e}")
    conn = None
    cursor = None

# -----------------------------------
# Helper: Map occurrence -> time period
# -----------------------------------
def map_time_period(occurrence):
    mapping = {
        "UM1": "Morning",
        "UM2": "Morning",
        "UN1": "Afternoon",
        "UN2": "Afternoon",
        "UE1": "Evening",
        "UE2": "Evening"
    }
    return mapping.get(occurrence, "Unknown")

# -----------------------------------
# Routes
# -----------------------------------
@app.route('/modules', methods=['GET'])
def get_modules():
    if not cursor:
        return jsonify({"error": "Database not connected"}), 500

    cursor.execute("""
        SELECT Module_Code, Module_Name, Occurrence, Faculty, Prerequisite,
               module_start_time, module_end_time
        FROM module_data
    """)
    rows = cursor.fetchall()

    modules = []
    for row in rows:
        modules.append({
            "id": row[0],
            "name": row[1],
            "occurrence": row[2],
            "faculty": row[3],
            "prerequisite": row[4],
            "start_time": str(row[5]),
            "end_time": str(row[6]),
            "time_period": map_time_period(row[2])
        })

    return jsonify(modules)

@app.route('/selected_modules', methods=['GET'])
def get_selected_modules():
    if not cursor:
        return jsonify({"error": "Database not connected"}), 500

    cursor.execute("""
        SELECT module_code, module_name, occurrence, faculty, prerequisite,
               module_start_time, module_end_time
        FROM selected_modules
    """)
    rows = cursor.fetchall()

    selected = []
    for row in rows:
        selected.append({
            "id": row[0],
            "name": row[1],
            "occurrence": row[2],
            "faculty": row[3],
            "prerequisite": row[4],
            "start_time": str(row[5]),
            "end_time": str(row[6]),
        })

    return jsonify(selected)

@app.route('/selected_modules/<module_code>', methods=['DELETE'])
def remove_selected_module(module_code):
    if not cursor:
        return jsonify({"error": "Database not connected"}), 500

    cursor.execute("DELETE FROM selected_modules WHERE module_code = %s", (module_code,))
    conn.commit()
    return jsonify({"message": "Module removed successfully"}), 200

@app.route('/modules', methods=['POST'])
def add_module():
    if not cursor:
        return jsonify({"error": "Database not connected"}), 500

    data = request.json

    module_code = data.get("id")
    module_name = data.get("name")
    occurrence = data.get("occurrence")
    faculty = data.get("faculty")
    prerequisite = data.get("prerequisite")
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    student_id = data.get("student_id", "unknown")

    cursor.execute("SELECT 1 FROM selected_modules WHERE module_code = %s", (module_code,))
    if cursor.fetchone():
        return jsonify({"message": f"{module_name} is already selected."}), 400

    cursor.execute("""
        SELECT module_code, module_name
        FROM selected_modules
        WHERE (CAST(%s AS TIME) < CAST(module_end_time AS TIME))
          AND (CAST(%s AS TIME) > CAST(module_start_time AS TIME))
    """, (end_time, start_time))

    clash = cursor.fetchone()
    if clash:
        return jsonify({"message": f"Time clash detected with {clash[1]} ({clash[0]})"}), 400

    cursor.execute("""
        INSERT INTO selected_modules (module_code, module_name, occurrence, faculty, prerequisite, module_start_time, module_end_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING module_code, module_name, occurrence, faculty, prerequisite, module_start_time, module_end_time
    """, (module_code, module_name, occurrence, faculty, prerequisite, start_time, end_time))

    new_module = cursor.fetchone()
    conn.commit()

    if producer:
        message = {
            "student_id": student_id,
            "module_code": module_code,
            "module_name": module_name,
            "occurrence": occurrence,
            "faculty": faculty,
            "start_time": start_time,
            "end_time": end_time
        }
        producer.send("module_selection_topic", message)
        producer.flush()

    return jsonify({
        "id": new_module[0],
        "name": new_module[1],
        "occurrence": new_module[2],
        "faculty": new_module[3],
        "prerequisite": new_module[4],
        "start_time": str(new_module[5]),
        "end_time": str(new_module[6])
    }), 201


# -----------------------------------
# Run Server
# -----------------------------------
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
