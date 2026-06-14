# Airport Congestion Prediction System
### End-to-End Data Engineering and Predictive Analytics Platform for Airport Congestion Estimation
---
#### Overview
The Airport Congestion Prediction System is an end-to-end data engineering and machine learning project designed to estimate airport congestion using historical aviation operations, passenger throughput, TSA wait times, and weather data.

The project integrates multiple large-scale public datasets through a PySpark-based ETL pipeline and applies machine learning techniques to predict TSA wait times, which serve as a proxy for airport congestion.

This project demonstrates modern data engineering practices, including:
- Distributed data processing with PySpark
- Multi-source data integration
- Cloud storage integration with Amazon S3
- Incremental ETL processing
- Feature engineering and predictive analytics
- Machine learning model development and evaluation
---
#### Business Problem

Passengers are commonly advised to arrive at airports using generalized recommendations, such as arriving two hours before domestic flights or three hours before international flights. These recommendations do not account for airport-specific operational conditions, passenger demand fluctuations, weather disruptions, or changing congestion patterns.

##### This project investigates whether historical operational data can be used to estimate airport congestion and support more informed and data-driven arrival recommendations.
---
#### Solution Architecture
##### Data Sources
- Bureau of Transportation Statistics (BTS)
    - Flight Performance Data
    - Airport Lookup Data
    - Airport On-Time Departure Performance Data
- Transportation Security Administration (TSA)
    - Passenger Throughput Data
    - TSA Wait Time Data
- National Oceanic and Atmospheric Administration (NOAA)
    - Historical Weather Data

##### Processing Pipeline
- Data Extraction and Ingestion
- Data Quality Validation
- Data Cleaning and Standardization
- Data Transformation and Aggregation
- Dataset Integration
- Airport Congestion Gold Dataset Creation
- Exploratory Data Analysis
- Machine Learning Model Development
- Model Evaluation and Interpretation
---
#### Architecture Diagram
![alt text](<Data Flow Architecture Diagram (L1) v0.2.jpg>)

##### The system architecture demonstrates how heterogeneous aviation datasets are ingested, transformed through a PySpark ETL pipeline, integrated into a unified analytical dataset, and consumed by machine-learning models for airport congestion estimation.
---
#### Technology Stack
##### Data Engineering
- Python
- PySpark
- Pandas
- NumPy
- Docker
- Amazon S3
- Parquet
##### Data Analytics & Machine Learning
- Scikit-Learn
- Matplotlib
- Seaborn
- Jupyter Notebook
##### Development Environment
- Visual Studio Code
- Dockerized Spark Environment
- AWS Command Line Interface
---
#### ETL Pipeline
The ETL pipeline processes millions of aviation records through several stages:

##### Extraction
- Ingested flight performance, TSA, airport, and weather datasets
- Processed CSV and Excel source files
##### Transformation
- Data quality validation
- Schema standardization
- Missing-value handling
- Incremental processing workflows
- Aggregation and enrichment operations
##### Loading
- Generated transformed Parquet datasets
- Created integrated airport congestion gold dataset
- Stored outputs locally and in Amazon S3
---
#### Airport Congestion Gold Dataset

The final analytical dataset combines airport operational metrics, passenger demand indicators, TSA wait times, and weather observations into a unified airport-month analytical structure.

Key features include:
- Total flight volume
- Average departure delay
- Average arrival delay
- Delay rates
- Cancellation rates
- Passenger throughput
- Average TSA wait time
- Weather metrics
- Passenger-to-flight ratio