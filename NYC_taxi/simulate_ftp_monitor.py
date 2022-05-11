import time, csv
from json import dumps
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class  MyHandler(FileSystemEventHandler):

    def  on_modified(self,  event):
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Event type: {event.event_type} path : {event.src_path}")

    def  on_deleted(self,  event):
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Event type: {event.event_type} path : {event.src_path}")

    def  on_created(self,  event):
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Event type: {event.event_type} path : {event.src_path}")
        
        TOPCIC_NAME = "StreamTaxi"
        BOOTSTRAP_SERVER = "localhost:9092"
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER,value_serializer=lambda x:dumps(x).encode('utf-8'))

        with open(event.src_path,  encoding='utf-8', errors='ignore') as file:
            column = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
                    "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                    "total_amount", "congestion_surcharge"]
            data = csv.DictReader(file, delimiter=",", fieldnames=column)
            for row in data:
                producer.send(TOPCIC_NAME,row)
            

if __name__ ==  "__main__":
    event_handler = MyHandler()
    observer = Observer()
    observer.schedule(event_handler,  path="/Users/huyle/Desktop/ftp",  recursive=True)
    observer.start()

    try:
        while  True:
            time.sleep(1)
    except  KeyboardInterrupt:
        observer.stop()
    observer.join()
