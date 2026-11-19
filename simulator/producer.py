
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"
TOPIC = "production-events"

MACHINES = {
    "M01": "Découpe",
    "M02": "Assemblage",
    "M03": "Soudage",
    "M04": "Emballage",
}


def generate_event(machine_id):
    # Une petite proportion des mesures sera anormale.
    anomaly = random.random() < 0.10

    temperature = random.uniform(95, 110) if anomaly else random.uniform(55, 85)
    vibration = random.uniform(7, 10) if anomaly else random.uniform(1, 4)

    return {
        "event_id": str(uuid.uuid4()),
        "machine_id": machine_id,
        "machine_type": MACHINES[machine_id],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temperature, 2),
        "vibration": round(vibration, 2),
        "pressure": round(random.uniform(3, 7), 2),
        "production_rate": random.randint(30, 60),
        "defect_count": random.randint(0, 3) if anomaly else 0,
        "status": "WARNING" if anomaly else "RUNNING",
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print("Simulateur démarré. Appuie sur Ctrl+C pour arrêter.")

    try:
        while True:
            for machine_id in MACHINES:
                event = generate_event(machine_id)

                producer.send(
                    TOPIC,
                    key=machine_id.encode("utf-8"),
                    value=event,
                )

                print(
                    f"{machine_id} | "
                    f"Temp: {event['temperature']}°C | "
                    f"Vibration: {event['vibration']} | "
                    f"Status: {event['status']}"
                )

            producer.flush()
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nArrêt du simulateur.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
