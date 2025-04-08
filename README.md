# Project Overview

This project focuses on processing and transforming data to construct a star schema database, facilitating efficient data analysis and reporting. The primary objectives include:

    Data Extraction and Transformation: Processing raw data from various sources to create structured fact and dimension tables.​

    Data Integration: Combining information from multiple DataFrames (sku_df, logs_df, market_price_df, and clean_df) to ensure consistency and accuracy.​

    Automation and Reproducibility: Utilizing Docker to containerize the data processing pipeline, ensuring consistent execution across different environments.​
    DEV Community

The implementation adheres to PEP 8 guidelines and incorporates logging for enhanced traceability and debugging.​
Thought Process

The development of this project was guided by the following considerations:

    Data Structuring: Designing fact and dimension tables based on the star schema to optimize query performance and simplify data analysis.​

    Modularity: Creating functions that handle specific tasks (e.g., data extraction, transformation, and loading) to enhance code readability and maintainability.​

    Scalability: Ensuring that the data processing pipeline can handle large datasets efficiently.​

    Portability: Containerizing the application with Docker to guarantee consistent behavior across various environments and simplify deployment.​
    Docker Documentation

Prerequisites

Before running the project, ensure that you have the following installed on your system:

    Docker: The application is containerized using Docker. Install Docker by following the instructions for your operating system on the official Docker website.​

Project Structure

The project directory is organized as follows:

project-root/
├── data/
│   ├── sku_data.csv
│   ├── logs_data.csv
│   └── market_price_data.csv
├── main.py
├── Dockerfile
├── docker-compose.yml
├── .gitignore
├── requirements.txt
├── Data Engineer Take Home Task.pdf
└── README.md

    data/: Contains the input data files.​

    scripts/: Contains the Python script (process_data.py) responsible for processing the data and generating the star schema tables.​
    DEV Community+1Medium+1

    Dockerfile: Defines the Docker image configuration.​

    README.md: Provides an overview of the project and instructions for setup and usage.​

Running the Project with Docker

To execute the data processing pipeline within a Docker container, follow these steps:

    Clone the Repository:

    git clone https://github.com/your-username/your-repository.git

    Navigate to the Project Directory:

    cd ferovinum


    Run the Docker Container:

    docker-compose up

    To Shutdown the Container:

    docker-compose down

This command starts the container, and the data processing script will execute, reading input files from the data directory and writing the resulting CSV files to the outputs directory.
Accessing the Output

After the container has finished running, the processed data will be available in the outputs directory. This directory will contain the following CSV files:​

    market_data.csv

    final_clean_dataset.csv: 

    most_profitable_brands.csv: 

    top_region_per_sku.csv: 

    two_most_profitable_brands.csv: 

These tables can be imported into a database or used directly for analysis and reporting.​
Notes

    Data Files: Ensure that the input data files (skus.json, logs.txt, and market_price_date.parquet) are placed in the data directory before running the container.​

    Docker Compatibility: The provided commands are compatible with Unix-based systems. For Windows, adjust the volume mounting syntax accordingly.​

    Logging: The data processing script includes logging to provide insights into the execution flow and assist with debugging if necessary.