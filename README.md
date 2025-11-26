# MY Healthcare Cloud Data Warehouse

End-to-end cloud data warehouse using  dbt, BigQuery, Docker and Airflow. This project involves a comprehensive data engineering workflow for analyzing healthcare capacity with respect to population data from data.gov.my. 
The final data spans from 2014 to 2022, as it is the latest available data on hospital bed and healthcare stafff in public sector.

Overview
<img width="1141" height="663" alt="dbt_bigquery drawio (5)" src="https://github.com/user-attachments/assets/c603862f-a0ef-4b46-9f1b-d0f021296ac5" />


Airflow schedules ingestion + dbt tasks.
<img width="1891" height="892" alt="DAG in Airflow Healthcare DW" src="https://github.com/user-attachments/assets/7aac9196-d499-4150-b52d-41ea8eb61b17" />


Data Source of healthcare staff, hospital bed and population from data.gov.my
<img width="1916" height="712" alt="image" src="https://github.com/user-attachments/assets/48b853d3-f796-4e09-820e-72d10f995bd3" />

Data ingestion
Python script will make an API call to retrieve the data and save it to google cloud storage.

Data Modeling  
dbt transforms raw data into clean marts.   
<img width="1860" height="920" alt="mart_healthcare_capacity_lineage_in_dbt" src="https://github.com/user-attachments/assets/a5d25b00-4c59-4a25-b4b7-393d7da318c5" />
  
<img width="1905" height="946" alt="Screenshot 2025-11-25 151321" src="https://github.com/user-attachments/assets/d4179d6f-73dd-4a9c-85f0-080c762ab029" />

Stages Created:
Staging: Preliminary data processing and preparation.  
Intermediate: Created DIM and FACT tables  
Data Mart: Using DIM and FACT tables, data marts are created for strategic use purposes such as the ratio of healthcare worker vs population based on benchmark from WHO.  

 BigQuery stores results for BI tools (Power BI).
 <img width="1095" height="872" alt="Connect PowerBI with Bigquery" src="https://github.com/user-attachments/assets/f1f61b41-d841-4abd-b6f3-cdb468457312" />
   
 <img width="1417" height="796" alt="Total of dr and nurses vs population" src="https://github.com/user-attachments/assets/42e5a8a2-1290-4a5f-9239-c0e7aefd77b7" />

 The benchmark ratio from WHO is 70.6 nurses per 10k population , while doctor is 20.7 per 10k population. The data source only give public healthcare staff while the benchmark from WHO is for all medical staff in the country. 
 From the chart, we can observe that the population, represented by the orange line, is increasing at a faster rate compared to the growth of medical staff. This imbalance creates a constraint on medical manpower capacity.

