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
    conn.execute("""
        DROP TABLE IF EXISTS inspections;
        CREATE TABLE IF NOT EXISTS inspections (
            camis TEXT,
            dba TEXT,
            boro TEXT,
            building TEXT,
            street TEXT,
            zipcode TEXT,
            phone TEXT,
            cuisine_description TEXT,
            inspection_date DATE,
            critical_flag TEXT,
            score INT,
            grade TEXT,
            grade_date DATE,
            violation_code TEXT,
            violation_description TEXT,
            inspection_type TEXT,
            latitude DOUBLE,
            longitude DOUBLE,
            bbl BIGINT
        )
    """)

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
            INSERT INTO inspections
            SELECT 
                json_extract_string(j.value, '$.camis'),
                json_extract_string(j.value, '$.dba'),
                json_extract_string(j.value, '$.boro'),
                json_extract_string(j.value, '$.building'),
                json_extract_string(j.value, '$.street'),
                json_extract_string(j.value, '$.zipcode'),
                json_extract_string(j.value, '$.phone'),
                json_extract_string(j.value, '$.cuisine_description'),
                json_extract(j.value, '$.inspection_date')::DATE,
                json_extract_string(j.value, '$.critical_flag'),
                json_extract(j.value, '$.score')::INT,
                json_extract_string(j.value, '$.grade'),
                json_extract(j.value, '$.grade_date')::DATE,
                json_extract_string(j.value, '$.violation_code'),
                json_extract_string(j.value, '$.violation_description'),
                json_extract_string(j.value, '$.inspection_type'),
                json_extract(j.value, '$.latitude')::DOUBLE,
                json_extract(j.value, '$.longitude')::DOUBLE,
                json_extract(j.value, '$.bbl')::BIGINT    
            FROM json_each(?) AS j
        """, [json.dumps(buffer)])

        print(f"Inserted batch of {len(buffer)} records into DuckDB.")

    except Exception as e:
        print("Error inserting into DuckDB:", e)

    buffer = [] # clear buffer after flush

# Setup Kafka Consumer
app = Application(
    broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
    consumer_group="nyc_inspections_consumer",
    processing_guarantee="at-least-once",
    auto_offset_reset="earliest",
    loglevel="ERROR"
)
# Define topic
topic = app.topic("nyc_inspections", value_deserializer="json")


# Start consuming messages
with app.get_consumer() as consumer:
    consumer.subscribe(["nyc_inspections"])
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
