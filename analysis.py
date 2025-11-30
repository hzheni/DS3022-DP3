# includes data cleaning, transformations, trend analysis, anomaly detection

import duckdb
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    filename='analysis.log'
)

DUCKDB_FILE = "inspections.duckdb"

class InspectionAnalyzer:
  
    """analyzing NYC restaurant inspections"""
    
    def __init__(self, db_file=DUCKDB_FILE):
        """initializing analyzer with DuckDB connection"""
        self.db_file = db_file
        self.conn = None
        self.logger = logging.getLogger(__name__)
        
    def connect(self):
        """establishing DuckDB connection"""
        try:
            self.conn = duckdb.connect(self.db_file)
            self.logger.info("Connected to DuckDB")
            print("Connected to DuckDB")
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            raise
    
    def close(self):
        """closing DuckDB connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Closed DuckDB connection")
    
    def get_table_summary(self):
        """getting summary statistics for all tables"""
        try:
            # checking inspections table
            insp_count = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM inspections"
            ).fetchall()[0][0]
            
            # checking pluto table
            pluto_count = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM pluto"
            ).fetchall()[0][0]
            
            # checking joined table
            joined_count = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM nyc_joined"
            ).fetchall()[0][0]
            
            print("Data Summary:\n")
            print(f"Inspections records: {insp_count:,}\n")
            print(f"PLUTO records: {pluto_count:,}\n")
            print(f"Joined records: {joined_count:,}\n")
            
            self.logger.info(f"Inspections: {insp_count}, PLUTO: {pluto_count}, Joined: {joined_count}")
            
        except Exception as e:
            self.logger.error(f"Error getting table summary: {e}")
            print(f"Error getting table summary: {e}")
    
    def clean_inspection_data(self):
      
        """data cleaning (missing data values, data type inconsistencies, invalid data records)"""
      
        try:
            print("cleaning data")
            
            # creating cleaned inspection table
            self.conn.execute("""
                CREATE OR REPLACE TABLE inspections_cleaned AS
                SELECT
                    camis,
                    dba,
                    boro,
                    zipcode,
                    LOWER(TRIM(cuisine_description)) as cuisine_description,
                    inspection_date,
                    CASE 
                        WHEN critical_flag = 'Y' THEN 1 
                        WHEN critical_flag = 'N' THEN 0 
                        ELSE NULL 
                    END as is_critical,
                    CASE 
                        WHEN score IS NOT NULL AND score >= 0 THEN score 
                        ELSE NULL 
                    END as score,
                    UPPER(TRIM(grade)) as grade,
                    grade_date,
                    LOWER(TRIM(violation_description)) as violation_description,
                    LOWER(TRIM(inspection_type)) as inspection_type,
                    bbl,
                    ROW_NUMBER() OVER (PARTITION BY camis, inspection_date ORDER BY grade_date DESC) as record_rank
                FROM inspections
                WHERE camis IS NOT NULL 
                  AND dba IS NOT NULL 
                  AND inspection_date IS NOT NULL
                  AND score IS NOT NULL
            """)
            
            # removing duplicates
            self.conn.execute("""
                CREATE OR REPLACE TABLE inspections_cleaned AS
                SELECT * EXCEPT (record_rank)
                FROM inspections_cleaned
                WHERE record_rank = 1
            """)
            
            cleaned_count = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM inspections_cleaned"
            ).fetchall()[0][0]
            
            print(f"Created cleaned inspection table: {cleaned_count:,} records")
            self.logger.info(f"Cleaned inspections table: {cleaned_count:,} records")
            
        except Exception as e:
            self.logger.error(f"Error cleaning inspection data: {e}")
            print(f"Error: {e}")
    
    def clean_pluto_data(self):
      
        """cleaning PLUTO property data (invalid/missing BBL, null values)"""
      
        try:
            print("cleaning PLUTO data")
            
            self.conn.execute("""
                CREATE OR REPLACE TABLE pluto_cleaned AS
                SELECT
                    LOWER(TRIM(address)) as address,
                    zipcode,
                    LOWER(TRIM(borough)) as borough,
                    CASE 
                        WHEN assesstot IS NOT NULL AND assesstot > 0 THEN assesstot 
                        ELSE NULL 
                    END as assesstot,
                    CASE 
                        WHEN yearbuilt > 1800 AND yearbuilt <= YEAR(NOW()) THEN yearbuilt 
                        ELSE NULL 
                    END as yearbuilt,
                    sanitboro,
                    policeprct,
                    comarea,
                    retailarea,
                    unitstotal,
                    bbl
                FROM pluto
                WHERE bbl IS NOT NULL 
                  AND bbl > 0
            """)
            
            pluto_cleaned_count = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM pluto_cleaned"
            ).fetchall()[0][0]
            
            print(f"Created cleaned PLUTO table: {pluto_cleaned_count:,} records")
            self.logger.info(f"Cleaned PLUTO table: {pluto_cleaned_count:,} records")
            
        except Exception as e:
            self.logger.error(f"Error cleaning PLUTO data: {e}")
            print(f"Error: {e}")
    
    def create_analysis_dataset(self):
      
        """creating unified analysis dataset joining the cleaned tables + calculating derived metrics and aggregate statistics"""
      
        try:
            print("creating analysis dataset")

            self.conn.execute("""
                CREATE OR REPLACE TABLE analysis_data AS
                SELECT
                    i.camis,
                    i.dba as restaurant_name,
                    i.boro as borough,
                    i.zipcode,
                    i.cuisine_description,
                    p.assesstot as property_value,
                    p.yearbuilt as year_built,
                    p.retailarea,
                    p.policeprct,
                    i.inspection_date,
                    i.is_critical,
                    i.score,
                    i.grade,
                    i.grade_date,
                    i.inspection_type,
                    YEAR(i.inspection_date) as inspection_year,
                    MONTH(i.inspection_date) as inspection_month,
                    i.violation_description,
                    CASE 
                        WHEN i.score < 14 THEN 'A (0-13)'
                        WHEN i.score < 28 THEN 'B (14-27)'
                        WHEN i.score >= 28 THEN 'C (28+)'
                        ELSE 'UNKNOWN'
                    END as score_category,
                    CASE 
                        WHEN p.yearbuilt IS NULL THEN NULL
                        WHEN DATEDIFF('day', MAKE_DATE(p.yearbuilt, 1, 1), i.inspection_date::DATE) < 0 THEN 0
                        ELSE DATEDIFF('day', MAKE_DATE(p.yearbuilt, 1, 1), i.inspection_date::DATE) / 365
                    END as building_age_at_inspection
                FROM inspections_cleaned i
                LEFT JOIN pluto_cleaned p ON i.bbl = p.bbl
                WHERE YEAR(i.inspection_date) >= 2015
            """)
            
            analysis_count = self.conn.execute(
                "SELECT COUNT(*) as cnt FROM analysis_data"
            ).fetchall()[0][0]
            
            print(f"Created analysis dataset: {analysis_count:,} records")
            self.logger.info(f"Analysis dataset: {analysis_count:,} records")
            
        except Exception as e:
            self.logger.error(f"Error creating analysis dataset: {e}")
            print(f"Error: {e}")
    
    def export_analysis_data(self):
      
        """exporting analysis data to CSV for visualization"""
      
        try:
            print("exporting data")
            
            # the main analysis dataset
            df = self.conn.execute("SELECT * FROM analysis_data").df()
            df.to_csv("analysis_data.csv", index=False)
            print(f"  ✓ Exported analysis_data.csv ({len(df):,} rows)")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error exporting data: {e}")
            print(f"Error: {e}")
            return None
    
    def get_cuisine_performance(self):
      
        """analyzing performance by cuisine type + returning cuisine statistics for visualization"""
      
        try:
            print("analyzing cuisine performance")
            
            query = """
            SELECT
                cuisine_description as cuisine,
                COUNT(DISTINCT camis) as num_restaurants,
                COUNT(*) as total_inspections,
                AVG(score) as avg_score,
                AVG(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) as critical_violation_rate,
                AVG(CASE WHEN grade = 'A' THEN 1 ELSE 0 END) as grade_a_rate,
                AVG(CASE WHEN grade = 'B' THEN 1 ELSE 0 END) as grade_b_rate,
                AVG(CASE WHEN grade = 'C' THEN 1 ELSE 0 END) as grade_c_rate,
                STDDEV(score) as score_std_dev
            FROM analysis_data
            WHERE cuisine_description IS NOT NULL
            GROUP BY cuisine_description
            HAVING COUNT(DISTINCT camis) >= 20
            ORDER BY avg_score DESC
            """
            
            results = self.conn.execute(query).df()
            print(f"Analyzed {len(results)} cuisine types")
            self.logger.info(f"Cuisine analysis: {len(results)} types")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in cuisine analysis: {e}")
            print(f"Error: {e}")
            return None
    
    def get_borough_comparison(self):
      
        """comparing inspection outcomes across NYC boroughs + identifying boroughs with highest violation rates"""

        try:
            print("analyzing borough comparison")
            
            query = """
            SELECT
                borough,
                COUNT(DISTINCT camis) as num_restaurants,
                COUNT(*) as total_inspections,
                AVG(score) as avg_score,
                MEDIAN(score) as median_score,
                AVG(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) as critical_violation_rate,
                AVG(CASE WHEN grade = 'A' THEN 1 ELSE 0 END) as grade_a_pct,
                AVG(CASE WHEN grade = 'B' THEN 1 ELSE 0 END) as grade_b_pct,
                AVG(CASE WHEN grade = 'C' THEN 1 ELSE 0 END) as grade_c_pct,
                COUNT(DISTINCT CASE WHEN is_critical = 1 THEN camis ELSE NULL END) as restaurants_with_critical_violations
            FROM analysis_data
            WHERE borough IS NOT NULL
            GROUP BY borough
            ORDER BY critical_violation_rate DESC
            """
            
            results = self.conn.execute(query).df()
            print(f"Borough comparison complete: {len(results)} boroughs")
            self.logger.info(f"Borough analysis complete")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in borough analysis: {e}")
            print(f"Error: {e}")
            return None
    
    def detect_temporal_trends(self):
      
        """detecting temporal trends in inspection scores and violations + identifying improving/declining neighborhoods"""
      
        try:
            print("detecting temporal trends")
            
            query = """
            SELECT
                inspection_year,
                inspection_month,
                COUNT(*) as num_inspections,
                AVG(score) as avg_score,
                AVG(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) as critical_rate,
                COUNT(DISTINCT camis) as num_restaurants_inspected
            FROM analysis_data
            GROUP BY inspection_year, inspection_month
            ORDER BY inspection_year, inspection_month
            """
            
            results = self.conn.execute(query).df()
            print(f"Identified temporal trends: {len(results)} time periods")
            self.logger.info(f"Temporal trends identified: {len(results)} periods")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in temporal trend analysis: {e}")
            print(f"Error: {e}")
            return None
    
    def detect_anomalies(self):
      
        """detecting restaurants with anomalous inspection patterns + using statistical methods to identify outliers (sudden score change, consistent violations, etc)"""
      
        try:
            print("detecting anomalies")
            
            # creating a query for anomalous restaurants
            query = """
            WITH restaurant_stats AS (
                SELECT
                    camis,
                    restaurant_name,
                    borough,
                    cuisine_description,
                    COUNT(*) as num_inspections,
                    AVG(score) as avg_score,
                    STDDEV(score) as score_std_dev,
                    MAX(score) - MIN(score) as score_range,
                    AVG(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) as critical_rate,
                    MIN(inspection_date) as first_inspection,
                    MAX(inspection_date) as last_inspection
                FROM analysis_data
                GROUP BY camis, restaurant_name, borough, cuisine_description
                HAVING COUNT(*) >= 5
            ),
            anomaly_scores AS (
                SELECT
                    *,
                    CASE 
                        WHEN score_std_dev > 10 THEN 'High Score Volatility'
                        WHEN critical_rate > 0.5 THEN 'Frequent Critical Violations'
                        WHEN avg_score > 50 THEN 'Consistently Poor Scores'
                        WHEN score_range > 40 THEN 'Extreme Score Swings'
                        ELSE 'Normal'
                    END as anomaly_type
                FROM restaurant_stats
            )
            SELECT
                *
            FROM anomaly_scores
            WHERE anomaly_type != 'Normal'
            ORDER BY critical_rate DESC, avg_score DESC
            LIMIT 500
            """
            
            results = self.conn.execute(query).df()
            print(f" Identified {len(results)} restaurants with anomalous patterns")
            self.logger.info(f"Anomalies detected: {len(results)} restaurants")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            print(f"Error: {e}")
            return None
    
    def property_value_analysis(self):
      
        """analyzing relationship between property value and inspection outcomes - key insight is property value vs. inspection performance"""
      
        try:
            print("analyzing property value impact")
            
            query = """
            SELECT
                CASE 
                    WHEN property_value IS NULL THEN 'Unknown'
                    WHEN property_value < 100000 THEN '$0-100K'
                    WHEN property_value < 500000 THEN '$100K-500K'
                    WHEN property_value < 1000000 THEN '$500K-1M'
                    WHEN property_value < 5000000 THEN '$1M-5M'
                    ELSE '$5M+'
                END as property_value_range,
                COUNT(DISTINCT camis) as num_restaurants,
                COUNT(*) as total_inspections,
                AVG(score) as avg_score,
                AVG(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) as critical_rate,
                AVG(CASE WHEN grade = 'A' THEN 1 ELSE 0 END) as grade_a_rate,
                STDDEV(score) as score_volatility
            FROM analysis_data
            WHERE property_value IS NOT NULL
            GROUP BY property_value_range
            ORDER BY 
                CASE 
                    WHEN property_value_range = '$0-100K' THEN 1
                    WHEN property_value_range = '$100K-500K' THEN 2
                    WHEN property_value_range = '$500K-1M' THEN 3
                    WHEN property_value_range = '$1M-5M' THEN 4
                    WHEN property_value_range = '$5M+' THEN 5
                    ELSE 6
                END
            """
            
            results = self.conn.execute(query).df()
            print(f"Property value analysis complete: {len(results)} segments")
            self.logger.info(f"Property value analysis: {len(results)} segments")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in property value analysis: {e}")
            print(f"Error: {e}")
            return None
    
    def violation_frequency_analysis(self):
      
        """analyzing most common violation types + identifying systemic issues in NYC restaurants"""
      
        try:
            print("analyzing violation patterns")
            
            query = """
            SELECT
                SUBSTRING(violation_description, 1, 100) as violation_category,
                COUNT(*) as frequency,
                COUNT(DISTINCT camis) as restaurants_affected,
                AVG(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) as is_critical_rate
            FROM analysis_data
            WHERE violation_description IS NOT NULL
            GROUP BY violation_description
            ORDER BY frequency DESC
            LIMIT 30
            """
            
            results = self.conn.execute(query).df()
            print(f"Identified {len(results)} major violation patterns")
            self.logger.info(f"Violation patterns: {len(results)} types")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in violation analysis: {e}")
            print(f"Error: {e}")
            return None
    
    def run_complete_analysis(self):
      
        """executing analysis pipeline"""
        try:
            print("\n" + "="*60)
            print("NYC RESTAURANT INSPECTIONS ANALYSIS")
            print("="*60)
            
            self.connect()
            self.get_table_summary()
            
            # data cleaning
            self.clean_inspection_data()
            self.clean_pluto_data()
            self.create_analysis_dataset()
            
            # exporting the cleaned data
            self.export_analysis_data()
            
            # analysis phase
            print("\n" + "="*60)
            print("ANALYSIS PHASE")
            print("="*60)
            
            cuisine_df = self.get_cuisine_performance()
            borough_df = self.get_borough_comparison()
            temporal_df = self.detect_temporal_trends()
            anomalies_df = self.detect_anomalies()
            property_df = self.property_value_analysis()
            violations_df = self.violation_frequency_analysis()
            
            # exporting results
            if cuisine_df is not None:
                cuisine_df.to_csv("cuisine_analysis.csv", index=False)
            if borough_df is not None:
                borough_df.to_csv("borough_analysis.csv", index=False)
            if temporal_df is not None:
                temporal_df.to_csv("temporal_trends.csv", index=False)
            if anomalies_df is not None:
                anomalies_df.to_csv("anomalies_detected.csv", index=False)
            if property_df is not None:
                property_df.to_csv("property_value_analysis.csv", index=False)
            if violations_df is not None:
                violations_df.to_csv("violation_frequency.csv", index=False)
            
            print("\n" + "="*60)
            print("ANALYSIS COMPLETE - all results have been exported to CSV files")
            print("="*60 + "\n")
            
            self.close()
            
            return {
                'cuisine': cuisine_df,
                'borough': borough_df,
                'temporal': temporal_df,
                'anomalies': anomalies_df,
                'property': property_df,
                'violations': violations_df
            }
            
        except Exception as e:
            self.logger.error(f"Error in complete analysis: {e}")
            print(f"Error: {e}")
            raise


if __name__ == "__main__":
    analyzer = InspectionAnalyzer()
    results = analyzer.run_complete_analysis()
