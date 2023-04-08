# Project

In this module we modernize our business case by transforming by creating a batch processing architecture. Here you will understand the use of Pipeline Orchestrators in a real and practical context.
You will carry out an end to end project using all the tools learned in the previous modules. You can apply the different phases of a project:

* Architecture Definition 
* Identification of sources
* Data Intake
* Information Modeling
* Transformation and Processing
* Orchestration of Data Pipelines
* Automation

**Case**

The Retail SA company needs to centralize business information and wants to build a data repository that allows it to learn more about its customers and make decisions to improve its service and operation.

**Details:**
1. Develop a basic Architecture for the development of the solution
2. Develop ETL processes
    * Extract from GCP, MongoDB, Azure Data Lake Storage
    * Transform Using pandas (Enunciados):
        * a. Top 5 customers with more orders
        * b. Top 5 states with the most orders processed
        * c. Quantity of Orders per month
        * d. What are the busiest days of the week?
        * e. Top 5 types of items (Products) best sellers
        * f. Top 5 customers with the highest turnover
    * Load to Azure Sql Server / MySQL

**Instructions:**
* Review the structure defined in the .drawio file
* Here we use a yaml file with connection strings and definitions inside config directory.
* We use that in utils within functions to get connections, clients, etc.
* The ipynb files are used for debugging purposes
* Must install packages in requirements.txt file
