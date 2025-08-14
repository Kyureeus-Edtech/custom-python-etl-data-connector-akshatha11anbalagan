Custom Python ETL Data Connector – Kyureeus EdTech (SSN CSE)
This repository contains my assignment for the Software Architecture course: building a custom Python ETL data connector to extract data from an API, transform it for compatibility, and load it into MongoDB.
Student: Akshatha Anbalagan
Roll Number: 3122225001007
Branch: AkshathaAnbalagan_3122225001007_A

📝 Overview
This project demonstrates a full ETL pipeline:
Extract: Connects to a public/open API endpoint and fetches JSON data.
Transform: Cleans, validates, and formats data for MongoDB storage.
Load: Inserts the transformed data into a MongoDB collection with ingestion timestamps for auditing.
Note: The API used in this project does not require authentication or API keys. It is a publicly accessible endpoint.

🔗 API Endpoint
Endpoint URL: http://bits.cybergreen.net/prod/stats/latest/count.csv
Type: Public CSV file, no authentication required
Response Format: CSV (Comma-Separated Values)

📊 Data Provided by CyberGreen CSV
The ETL pipeline fetches data from the CyberGreen CSV file and stores it in MongoDB in the following format:
Metrics: Numeric counts for various categories of cybersecurity incidents. Example values: 2, 3, 82, 123, 20330, 37629.
Dates: Each record includes a start and end date for the data coverage (e.g., 2018-07-03 to 2021-07-27).
Countries: Source and target country codes (e.g., US, NG) indicating the geographic scope of the data.
MongoDB Metadata: Each document has a unique _id and an ingestion_ts timestamp recording when the data was inserted (e.g., 2025-08-14T12:55:55.342+00:00).
This structure ensures that the data is ready for analysis, auditing, and historical tracking of cybersecurity statistics.

🔧 Implementation Details
ETL Pipeline:
Extract: Uses pandas to read the CSV file from the public URL.
Transform: Cleans and formats data to ensure compatibility with MongoDB.
Load: Inserts records into a MongoDB collection named cybergreen_raw, adding ingestion timestamps.
Error Handling & Validation:
Handles missing values, empty rows, or connectivity issues.
Ensures smooth insertion into MongoDB without duplicates or corrupted data.

✅ Steps to Run

Clone your branch:
git clone https://github.com/Kyureeus-Edtech/custom-python-etl-data-connector-akshatha11anbalagan.git
git checkout AkshathaAnbalagan_3122225001007_A

Install dependencies:
pip install -r requirements.txt
Run the ETL script:
python etl_connector.py
Verify data in MongoDB collection cybergreen_raw.

📚 References
PyMongo Documentation
pandas Documentation
CyberGreen CSV

💡 Summary
This ETL connector demonstrates a complete pipeline from a public CyberGreen CSV to MongoDB, with robust error handling, data validation, and a clean project structure. The connector is ready for extension to other APIs, including those requiring authentication.