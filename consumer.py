from kafka import KafkaConsumer
import psycopg2
import json
import time

# Connect to PostgreSQL
while True:
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="mod_system",
            user="postgres",
            password="twitter77"
        )
        cursor = conn.cursor()
        print("Connected to PostgreSQL")
        break
    except Exception as e:
        print("Waiting for PostgreSQL...", e)
        time.sleep(5)

# Create Kafka consumer
consumer = KafkaConsumer(
    'module_selection_topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='module_consumers'
)

print("Kafka consumer started...")

for message in consumer:
    data = message.value
    print(f"Received message: {data}")

    # Insert into selected_modules table
    try:
        cursor.execute("""
            INSERT INTO selected_modules 
            (module_code, module_name, occurrence, faculty, module_start_time, module_end_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            data['module_code'],
            data['module_name'],
            data['occurrence'],
            data['faculty'],
            data['start_time'],
            data['end_time']
        ))
        conn.commit()
        print(f" Inserted module {data['module_code']} for {data['student_id']}")
    except Exception as e:
        print(f" Error inserting: {e}")
        conn.rollback()
