# DS3022-DP3: NYC Restaurant Inspections & PLUTO Streaming and Data Analysis

## Team Members
Jessica Ni & Jolie Ng

## Project Summary
This project combines NYC Restaurant Inspection results with building characteristics from the PLUTO dataset to explore patterns in restaurant inspection scores, grades, and other attributes. By joining these datasets, we can analyze how building characteristics and neighborhood factors relate to restaurant inspection outcomes.

## Data Sources
Both datasets were accessed via the **NYC Open Data Socrata API**:

1. **NYC Restaurant Inspection Results**  
   - Provides inspection records, scores, grades, and violations for restaurants across New York City.  

2. **NYC Primary Land Use Tax Lot Output (PLUTO)**  
   - Provides building characteristics including year built, lot size, and total units.  

## How to Run This Repository

### 1. Install requirements
Ensure requirements are met by installing packages in `requirements.txt`

### 2. Load the raw data (streaming pipeline)
**NYC Restaurant Inspection Results**

Run these two scripts at the same time, each in its own terminal/tab 

`producer_inspections.py` and `consumer_inspections.py` 

The producer streams the restaurant inspection API, and the consumer receives the stream and writes the records into a DuckDB file, inspections.db. 

**NYC Primary Land Use Tax Lot Output (PLUTO)**

Run these two scripts at the same time, each in its own terminal/tab 

`producer_pluto.py` and `consumer_pluto.py` 

The producer streams the building characteristics API, and the consumer receives the stream and writes the records into the same DuckDB file, under a different table. 

### 3. Join two datasets
Run `prep_data.py` to join both DuckDB tables based on `BBL` (Bourough-Block-Lot) and check the total number of successes from the join.

### 4. Cleaning and analysis
Run `analysis.py` to clean the data, create analysis tables, and export cleaned datasets to csv for visualization.

### 5. Visualization
Run `visualization.py` to generate key findings and plots.
