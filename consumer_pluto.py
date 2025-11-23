from quixstreams import Application
import duckdb
import json

BATCH_SIZE = 10000
PRINT_EVERY = 5000
DUCKDB_FILE = "inspections.duckdb"

buffer = [] # temporarily store messages before batch insert
total_consumed = 0

# Connect to DuckDB and create table with corresponding columns
try:
    conn = duckdb.connect(DUCKDB_FILE)

    # Create table if it does not exist
    # Columns for JOIN later: latitude, longitude
    # Columns for analysis: zipcode, borough, assesstot, yearbuilt, comarea, retailarea
    conn.execute("""
                DROP TABLE IF EXISTS pluto;
                CREATE TABLE IF NOT EXISTS pluto (
                address TEXT,
                zipcode TEXT,
                borough TEXT,
                latitude DOUBLE,
                longitude DOUBLE,

                assesstot DOUBLE,
                yearbuilt INTEGER,
                sanitboro INTEGER,
                policeprct INTEGER,
                comarea DOUBLE,
                retailarea DOUBLE,
                unitstotal INTEGER,
                bbl BIGINT); """)

    print("Connected to DuckDB and ensured table exists.")

except Exception as e:
    print("Failed to connect to DuckDB or create table:", e)
    raise

# Insert batch into DuckDB
def flush_to_duckdb():
    """Insert batch into DuckDB."""
    global buffer

    if not buffer:
        return
    
    # For each column, extract from JSON and insert into table
    try:
        conn.execute("""
                    INSERT INTO pluto
                    SELECT
                    json_extract_string(j.value, '$.address'),
                    json_extract_string(j.value, '$.zipcode'),
                    json_extract_string(j.value, '$.borough'),
                    json_extract(j.value, '$.latitude')::DOUBLE,
                    json_extract(j.value, '$.longitude')::DOUBLE,
                    json_extract(j.value, '$.assesstot')::DOUBLE,
                    json_extract(j.value, '$.yearbuilt')::INT,
                    json_extract(j.value, '$.sanitboro')::INT,
                    json_extract(j.value, '$.policeprct')::INT,
                    json_extract(j.value, '$.comarea')::DOUBLE,
                    json_extract(j.value, '$.retailarea')::DOUBLE,
                    json_extract(j.value, '$.unitstotal')::INT,
                    (json_extract(j.value, '$.bbl')::DOUBLE)::BIGINT
                    FROM json_each(?) AS j;""", [json.dumps(buffer)])

        print(f"Inserted batch of {len(buffer)} records into DuckDB.")

    except Exception as e:
        print("Error inserting into DuckDB:", e)

    buffer = [] # clear buffer after flush

# Setup Kafka Consumer
app = Application(
    broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
    consumer_group="nyc_pluto_consumer",
    processing_guarantee="at-least-once",
    auto_offset_reset="earliest",
    loglevel="ERROR"
)
# Define topic
topic = app.topic("nyc_pluto", value_deserializer="json")


# Start consuming messages
with app.get_consumer() as consumer:
    consumer.subscribe(["nyc_pluto"])
    print("Consumer started... waiting for messages.")

    # Consume messages in a loop
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print("Consumer error:", msg.error())
                continue

            raw = msg.value()

            # Already deserialized to dict?
            if isinstance(raw, dict):
                record = raw
            else:
                try:
                    record = json.loads(raw)
                except Exception:
                    print("Skipping invalid JSON message.")
                    continue

            if not isinstance(record, dict):
                print("Skipping non-dict message:", record)
                continue

            # Add to batch
            buffer.append(record)
            total_consumed += 1

            # Progress output
            if total_consumed % PRINT_EVERY == 0:
                print(f"Consumed {total_consumed} messages so far...")

            # If batch full, flush
            if len(buffer) >= BATCH_SIZE:
                flush_to_duckdb()

            # Store offsets
            consumer.store_offsets(msg)

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        flush_to_duckdb()
        conn.close()
