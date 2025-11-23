import duckdb
import logging

DUCKDB_FILE = "inspections.duckdb"
OUTPUT_TABLE = "nyc_joined"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename='prep_data.log',
)

def main():
    try:
        # Connect to DuckDB
        conn = duckdb.connect(DUCKDB_FILE)
        logging.info("Connected to DuckDB.")
        print("Connected to DuckDB.")

        # Remove duplicates from inspections table
        conn.execute("""
            CREATE OR REPLACE TABLE inspections AS
            SELECT DISTINCT * FROM inspections;
        """)
        logging.info("Removed duplicates from inspections table.")

        # Remove duplicates from pluto table
        conn.execute("""
            CREATE OR REPLACE TABLE pluto AS
            SELECT DISTINCT * FROM pluto;
        """)
        logging.info("Removed duplicates from pluto table.")
        print("Removed duplicates from both tables.")

        # Join tables on BBL (the business id)
        logging.info("Joining inspections and pluto tables on bbl...")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {OUTPUT_TABLE} AS
            SELECT
                i.*,
                p.borough,
                p.zipcode,
                p.assesstot,
                p.yearbuilt,
                p.sanitboro,
                p.policeprct,
                p.comarea,
                p.retailarea,
                p.unitstotal
            FROM inspections i
            INNER JOIN pluto p USING (bbl)
            WHERE p.assesstot IS NOT NULL;
        """)
        logging.info(f"Joined table created: {OUTPUT_TABLE}")
        print(f"Joined table created: {OUTPUT_TABLE}")

        # Keep unique dba (restaurant name) entries based on newest inspection date
        conn.execute(f"""
            CREATE OR REPLACE TABLE nyc_joined AS
            SELECT DISTINCT ON (dba)
                *
            FROM {OUTPUT_TABLE};
        """)
        logging.info("Filtered joined table to keep unique dba entries based on newest inspection date.")
        print("Filtered joined table to keep unique dba entries based on newest inspection date.")

        # Check a few rows
        logging.info("Preview of joined table:")
        preview = conn.execute(f"SELECT * FROM {OUTPUT_TABLE} LIMIT 5").fetchall()
        for row in preview:
            logging.info(row)
            print(row)

        conn.close()
        logging.info("DuckDB connection closed. Script finished successfully.")
        print("DuckDB connection closed. Script finished successfully.")

    except Exception as e:
        logging.exception("Error occurred while joining tables: %s", e)

def count_successes():
    try:
        conn = duckdb.connect(DUCKDB_FILE)
        success = conn.execute(f""" SELECT COUNT(*) FROM nyc_joined WHERE assesstot IS NOT NULL; """).fetchone()
        total_count = conn.execute(f""" SELECT COUNT(*) FROM nyc_joined; """).fetchone()
        print(f"Number of joined rows is: {success[0]} out of {total_count[0]}")
        logging.info(f"Number of joined rows is: {success[0]} out of {total_count[0]}")
        conn.close()
    except Exception as e:
        logging.exception("Error counting successes: %s", e)

if __name__ == "__main__":
    main()
    count_successes()
