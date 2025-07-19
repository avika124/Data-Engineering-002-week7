Data Lake File Processing Pipeline
Azure Data Factory pipeline to process CSV files from Data Lake Storage and load into SQL Database with date extraction from filenames.
Architecture
Data Lake Storage Gen2 → Azure Data Factory → SQL Database
File Types Processed
1. CUST_MSTR Files

Pattern: CUST_MSTR_YYYYMMDD.csv
Transform: Extract date from filename → date column (YYYY-MM-DD)
Table: CUST_MSTR

2. master_child_export Files

Pattern: master_child_export-YYYYMMDD.csv
Transform: Extract date → date (YYYY-MM-DD) + date_key (YYYYMMDD)
Table: master_child

3. H_ECOM_ORDER Files

Pattern: H_ECOM_ORDER.csv
Transform: Load as-is
Table: H_ECOM_Orders

Azure Resources

Resource Group: internship
Storage: avikadatalake2024
SQL Server: avika-sql-server-2025
Database: DataWarehouse
Data Factory: avika-data-factory-2025

Pipeline Flow

Get Metadata → List all files
Filter → Separate by file type
ForEach → Process each file
Copy Data → Transform and load
Truncate-Load → Fresh data daily

Database Tables
sql-- CUST_MSTR Table
CREATE TABLE CUST_MSTR (
    customer_id INT,
    customer_name NVARCHAR(255),
    email NVARCHAR(255),
    phone NVARCHAR(50),
    city NVARCHAR(100),
    state NVARCHAR(10),
    zipcode NVARCHAR(20),
    date DATE  -- From filename
);

-- master_child Table  
CREATE TABLE master_child (
    parent_id INT,
    child_id INT,
    relationship_type NVARCHAR(50),
    created_by NVARCHAR(100),
    status NVARCHAR(50),
    date DATE,       -- From filename (YYYY-MM-DD)
    date_key INT     -- From filename (YYYYMMDD)
);

-- H_ECOM_Orders Table
CREATE TABLE H_ECOM_Orders (
    order_id INT,
    customer_id INT,
    product_name NVARCHAR(255),
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    order_status NVARCHAR(50),
    payment_method NVARCHAR(50)
);
Key Features

✅ Automated file detection by pattern
✅ Date extraction from filenames
✅ Truncate-load pattern
✅ Multiple file support
✅ Error handling & logging
