from confluent_kafka import Consumer, Producer
import json
from database import DatabaseManager

class CDCProcessor:
    def __init__(self, kafka_broker, source_topic, database_manager):
        self.consumer = Consumer({
            'bootstrap.servers': kafka_broker,
            'group.id': 'cdc-consumer-group',
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({
            'bootstrap.servers': kafka_broker
        })
        self.source_topic = source_topic
        self.db_manager = database_manager
    
    def process_changes(self):
        self.consumer.subscribe([self.source_topic])
        
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                
                # Procesar payload de Debezium
                payload = json.loads(msg.value().decode('utf-8'))
                self.handle_change(payload)
        
        except KeyboardInterrupt:
            self.consumer.close()
    
    def handle_change(self, payload):
        # Lógica de procesamiento de cambios
        if payload['op'] == 'c':  # Create
            after = payload['after']
            self.db_manager.add_product(
                name=after['name'],
                price=after['price'],
                stock=after['stock']
            )
            print(f"Producto creado: {after['name']}")

def main():
    db_manager = DatabaseManager(
        'postgresql://postgres:postgres@localhost/inventorydb'
    )
    
    cdc_processor = CDCProcessor(
        kafka_broker='localhost:9092',
        source_topic='dbserver1.public.products',
        database_manager=db_manager
    )
    
    cdc_processor.process_changes()

if __name__ == "__main__":
    main()