from quixstreams import Application
import json
import requests
import time

NYC_PLUTO_URL = "https://data.cityofnewyork.us/resource/64uk-42ks.json"

def produce_to_kafka():
    offset = 0
    max_events = 250000 # Set a maximum of events for testing, make higher when everything works -> 100,000
    limit = 1000 
    total_produced = 0

    # Create the Application and producer once
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="DEBUG",
        producer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
        )
    with app.get_producer() as producer:
        while total_produced < max_events: # Produce until we reach max_events
            # Fetch batch from API
            params = {"$limit": limit, "$offset": offset}
            response = requests.get(NYC_PLUTO_URL, params=params)
            data = response.json()

            if not data:
                print("No more data from API!")
                break

            # Calculate how many more events we can produce
            remaining = max_events - total_produced
            batch = data[:remaining]
            # Produce each record
            for record in batch:
                # Ensure record is a dict
                if isinstance(record, dict):

                    try:
                        producer.produce(
                            topic="nyc_pluto", # My topic
                            value=json.dumps(record).encode("utf8")
                        )
                        total_produced += 1
                    except Exception as e:
                        print(f"Error producing record: {e}")
                else:
                    print("Skipping non-dict record:", record)

            # Flush batch to Kafka
            producer.flush()

            # Update offset for next API request
            offset += len(data)

            # Print total produced so far
            print(f"Total records produced so far: {total_produced}")

            # Sleep to avoid hitting API limits
            time.sleep(1)

if __name__ == "__main__":
    produce_to_kafka()
