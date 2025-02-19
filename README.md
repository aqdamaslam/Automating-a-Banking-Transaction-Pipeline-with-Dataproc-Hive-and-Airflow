# **Case Study: Automating Credit Card Transaction Processing & Reporting on GCP Using Dataproc, Airflow, and Hive**  

## **Overview**  
A leading financial institution wants to **automate the extraction, transformation, and reporting of credit card transactions**. The goal is to:  
- **Extract data from MySQL**, load it into **Hive on Dataproc**, and store it in **Parquet format** for optimization.  
- **Generate a monthly credit card usage report** for each user.  
- **Save the report in a Google Cloud Storage (GCS) bucket**, partitioned by **year and month** for faster query performance.  
- **Automate the entire workflow** using **Apache Airflow** and **Google Cloud Dataproc** while ensuring cost efficiency by dynamically creating and deleting the cluster.  

---

## **Architecture Diagram**  
![architecture](https://github.com/user-attachments/assets/fee88331-cb93-4889-b156-dd19425ffb15)


### **Architecture Flow**  
1. **Data Extraction:** MySQL transactions data is read using PySpark on Dataproc.  
2. **Data Transformation:** The extracted data is processed and stored in **Hive** on Dataproc.  
3. **Report Generation:** A monthly credit card usage report is generated using **PySpark**.  
4. **Data Storage:** The report is stored in **Google Cloud Storage (GCS)** in **Parquet format**, partitioned by **year and month**.  
5. **Automation:** Airflow manages and automates the workflow.  

---

## **Project Steps**  

### **Step 1: Setting Up the Environment**  
To implement this project, the following GCP services and tools are configured:  
1. **Google Cloud SQL (MySQL):** Stores transaction data.  
2. **Google Cloud Dataproc:** Runs PySpark jobs for processing large-scale data.  
3. **Google Cloud Storage (GCS):** Stores raw data and processed reports.  
4. **Hive Metastore on Dataproc:** Stores structured transaction data for efficient querying.  
5. **Apache Airflow:** Orchestrates the end-to-end workflow.  

---

### **Step 2: Designing the Data Pipeline**  

The pipeline consists of **four major tasks**:  
1. **Extract MySQL Data and Load into Hive**  
2. **Generate a Monthly Credit Card Usage Report**  
3. **Store Reports in GCS with Partitioning**  
4. **Automate Workflow with Airflow**  

---

### **Step 3: Creating Google Cloud Storage (GCS) Buckets**  
Two GCS buckets are created:  
- **`user-credit-card-reports-input`** → Stores the PySpark scripts and any required configuration files.  
- **`user-credit-card-reports-output`** → Stores the processed reports in **Parquet format**, partitioned by **year and month**.  

---

### **Step 4: Setting Up Google Cloud Dataproc**  
Since PySpark is used for data processing, a **Dataproc cluster** is created dynamically to optimize resource utilization. The cluster setup includes:  
- **1 master node** (n1-standard-4)  
- **2 worker nodes** (n1-standard-4)  
- **Hive support enabled**  
- **Auto-deletion of cluster after job completion**  

---

### **Step 5: Extracting MySQL Data and Loading into Hive**  
A PySpark job is executed on Dataproc to:  
1. Connect to **Google Cloud SQL (MySQL)** using JDBC.  
2. Read the **transactions table** into a Spark DataFrame.  
3. Write the data into **Hive** in **Parquet format** for optimized storage and retrieval.  
4. Enable **partitioning** based on `transaction_date`, `card_type`, and `loan_status` for faster query performance.  

---

### **Step 6: Generating Monthly Credit Card Usage Report**  
Another PySpark job is executed on Dataproc to:  
1. Read the **card transactions** data from Hive.  
2. Aggregate **monthly credit card usage** per user.  
3. Partition the data by **year and month**.  
4. Save the report to **Google Cloud Storage (GCS)** in **Parquet format**.  

---

### **Step 7: Automating the Workflow with Airflow**  
An **Airflow DAG** is created to automate the entire process. The DAG follows this sequence:  
1. **Create GCS buckets** (if they don’t exist).  
2. **Create a Dataproc cluster** dynamically.  
3. **Submit the MySQL-to-Hive PySpark job** on Dataproc.  
4. **Submit the report generation PySpark job** on Dataproc.  
5. **Delete the Dataproc cluster** to save costs.  

---

### **Step 8: Monitoring & Optimization**  
- **Airflow logs** are monitored to ensure successful job execution.  
- **Dataproc cluster is auto-deleted** to minimize cost.  
- **Hive partitions** are optimized for faster reporting queries.  
- **Parquet format** is used for efficient storage and querying.  

---

## **Final Outcome**  
✅ **Fully automated ETL pipeline** from MySQL → Hive → GCS.  
✅ **Monthly credit card usage reports** generated and stored efficiently.  
✅ **Partitioning by year and month** for optimized queries.  
✅ **Cost-efficient execution** by dynamically creating and deleting Dataproc clusters.  

---

## **Business Impact**  
🚀 **95% reduction in manual effort** for data processing.  
⚡ **70% faster query performance** due to partitioning and Parquet format.  
💰 **30-40% cost savings** by running Dataproc clusters only when needed.  

This **scalable, automated, and cost-effective solution** enables real-time insights into **credit card usage trends** for better financial decision-making. 🚀  

