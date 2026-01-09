# 🛒 End-to-End Data Warehouse: Sales Data Mart
### *From Raw CSVs to Analytics-Ready Data Warehouse using SQL Server*

![Overview](pics/Data_warehouse_overview.png)

---

## 🚀 **Project Overview**
**The Goal:** Transform disjointed sales data from two different systems (ERP & CRM) into a centralized, scalable Data Warehouse to unlock 360° customer views and analyze product performance.

**The Solution:** Built a robust ETL pipeline using the **Medallion Architecture** (Bronze, Silver, Gold) in SQL Server. The system handles data quality issues, historization, and logic integration to feed a Star Schema optimized for BI reporting.

**Key Metrics:**
* **Source Systems:** 2 (CRM & ERP)
* **Architecture:** Medallion (Bronze $\to$ Silver $\to$ Gold)
* **Tech Stack:** SQL Server (T-SQL), Excel/CSV, Draw.io

---

## 🏗️ **Architecture & Data Flow**

I designed a **Medallion Architecture** to ensure data traceability and quality.

![Data Flow Chart](docs/Data_Flow_chart.drawio.png)

1.  **Bronze Layer:** Raw ingestion (Full Load).
2.  **Silver Layer:** Cleansed, standardized, and deduplicated data.
3.  **Gold Layer:** Business-ready Star Schema (Facts & Dimensions).

---

## 🛠️ **Engineering Implementation**

### **1. The Bronze Layer: Raw Ingestion**
*Focus: Traceability & Performance*
* **Bulk Loading:** Utilized `BULK INSERT` for high-performance data loading from CSVs.
* **Idempotency:** Re-runnable pipelines using `TRUNCATE` before loading to prevent duplicate data buildup.
* **Metadata:** Added `_ingest_date` to track when data entered the warehouse.

### **2. The Silver Layer: Transformation & Quality**
*Focus: Deduplication, History, and Logic*
This layer houses the core engineering logic. I wrote stored procedures to handle specific data challenges found in the source:

* **Handling History (SCD Type Logic):**
    Used `LEAD()` window functions to calculate the `end_date` of product records based on the `start_date` of the next record, fixing gaps in historical product tracking.
    ```sql
    -- Example: Logic to fix historical gaps
    CASE 
        WHEN prd_end_dt < prd_start_dt 
        THEN CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
        ELSE prd_end_dt 
    END AS prd_end_dt
    ```

* **Data Validation Rules:**
    Identified and corrected invalid sales records where `Sales != Quantity * Price`.

* **Deduplication:**
    Applied `RANK() OVER (PARTITION BY ...)` to identify the most recent customer record and filter out older duplicates, ensuring a "Single Source of Truth".

* **Standardization:**
    Normalized country codes (e.g., 'DE' $\to$ 'Germany') and gender values using standardized `CASE` logic.

### **3. The Gold Layer: Dimensional Modeling**
*Focus: Analytics & Reporting*
Designed a **Star Schema** to enable fast aggregation in BI tools.

![Star Schema](docs/Sales_Data_Mart-Star_schema.drawio.png)

* **Fact Table:** `fact_sales` (Transactions)
* **Dimensions:** `dim_customers`, `dim_products`
* **Surrogate Keys:** Generated independent keys (e.g., `customer_key`) to decouple the warehouse from source system IDs.
* **Cross-System Integration:** Implemented "Fallback Logic" for customer gender—prioritizing CRM data but falling back to ERP data if the CRM value is NULL.

---

## 🔄 **Standardization & Reusability**
*My Philosophy: "Build once, improve continuously."*

I believe in reducing friction for future projects by creating reusable assets. Instead of treating this as a one-off script, I developed comprehensive **Technical Protocols** (SOPs) for every layer of the warehouse. This ensures that any future data source can be integrated with the same consistency and speed.

| Bronze Layer Protocol | Silver Layer Protocol | Gold Layer Protocol |
| :---: | :---: | :---: |
| ![Bronze](pics/bronze_layer.png) | ![Silver](pics/silver_layer.png) | ![Gold](pics/gold_layer.png) |
| *[Standardized Ingestion Checklist]* | *[Quality & Cleansing Framework]* | *[Modeling & Business Mapping]* |

---

## 📊 **Reporting Views**
Instead of exposing complex joins to end-users, I created an abstraction layer of Views for common business questions.

### **Customer Report View (`gold.report_customers`)**
*Consolidated view for marketing segmentation.*
* **Segmentation:** Automatically categorizes customers into `VIP`, `Regular`, or `New` based on spending history and lifespan.
* **Age Groups:** Dynamically buckets customers into age ranges (e.g., '20-29', '30-39').

### **Product Report View (`gold.report_products`)**
*Consolidated view for inventory management.*
* **Performance:** Calculates `Avg_Monthly_Sales` and identifies "High Performers" vs "Low Performers."
* **Lifespan:** Derives product longevity in months to normalize revenue comparisons.

---

## 🔍 **Advanced Analysis**
This repository focuses on the **Data Engineering** and **Architecture** aspects. 

👉 **[View the Advanced Exploratory Data Analysis (EDA) & Business Insights project](https://github.com/kopacm/SQL-Data-Analysis-Project/blob/main/README.md)**

---

## 👏 **Acknowledgements & Learning**
This project was inspired by the guidance of **Data with Baraa**.
* **Mentorship:** I followed the **[Data with Baraa YouTube Series](https://www.youtube.com/watch?v=9GVqKuTVANE&list=PLNcg_FV9n7qaUWeyUkPfiVtMbKlrfMqA8)** to understand the core concepts of Data Warehousing.
* **Execution:** I practiced by building this project from scratch—not just watching, but coding, debugging, and extending the original concepts with my own protocols and documentation to ensure practical mastery.

---

## 📬 **Contact**
**Miroslav Kopac** *Data Analyst* [LinkedIn](https://www.linkedin.com/in/miroslavkopac/) | [Email](mailto:kopacmiroslav@gmail.com)
