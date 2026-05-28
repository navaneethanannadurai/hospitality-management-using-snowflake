# Hospitality ETL Pipeline using Snowflake

A complete **Hospitality ETL (Extract, Transform, Load) Pipeline** built using **Python, Pandas, and Snowflake**.
This project processes hospitality data such as guest details, room information, and check-in/check-out records to generate analytical insights and business KPIs.

---

# 📌 Project Overview

This project demonstrates an end-to-end ETL workflow for the hospitality domain.

The pipeline performs:

* Extracting data from CSV files
* Validating and cleaning data
* Transforming raw data into enriched business data
* Loading data into Snowflake staging and final tables
* Maintaining audit logs
* Running analytical SQL queries
* Generating hotel business KPIs using Stored Procedures

---

# 🛠️ Technologies Used

* Python
* Pandas
* Snowflake
* SQL
* Snowflake Stored Procedures (JavaScript)
* CSV Files

---

# 📂 Project Structure

```bash
Hospitality-ETL-Pipeline/
│
├── hospitalityusecase.py
├── snowflake code.txt
├── Guest_Master_Hospitality.csv
├── Room_Master_Hospitality.csv
├── Checkin_Checkout_Hospitality.csv
├── README.md
```

---

# 📊 Data Files Used

| File Name                        | Description              |
| -------------------------------- | ------------------------ |
| Guest_Master_Hospitality.csv     | Guest master data        |
| Room_Master_Hospitality.csv      | Room details             |
| Checkin_Checkout_Hospitality.csv | Stay transaction records |

---

# ⚙️ Features Implemented

## ✅ ETL Pipeline

* Extract CSV data
* Validate records
* Transform business data
* Load into Snowflake

---

## ✅ Data Validation

Checks performed:

* Missing Guest IDs
* Missing Room IDs
* NULL Check-in dates
* Invalid checkout dates
* Negative amount validation
* Active stay detection

---

## ✅ Data Transformation

* Guest full name creation
* Stay duration calculation
* Revenue calculation
* Data enrichment using joins

---

## ✅ Snowflake Operations

* Database creation
* Schema creation
* Staging tables
* Final warehouse table
* MERGE / UPSERT logic
* Stored Procedures
* Audit Logging

---

## ✅ Analytical Use Cases

### 1. Daily Occupancy & Revenue Summary

* Total rooms
* Occupied rooms
* Occupancy %
* Revenue generated

---

### 2. Room Type Performance

* Room-wise revenue
* ADR (Average Daily Rate)
* Booking counts

---

### 3. Guest Stay Duration Analysis

* Average stay duration
* Total guest nights
* Long stay analysis

---

### 4. High Value Guest Identification

* Top revenue-generating guests
* Average spend
* Visit frequency

---

### 5. Booking Source Distribution

* Online vs Offline bookings
* Revenue by source

---

### 6. Payment Mode Analysis

* Payment trends
* Revenue by payment method

---

# 🧱 Snowflake Architecture

```text
CSV Files
   ↓
Python ETL Pipeline
   ↓
Snowflake Staging Tables
   ↓
Transformation & Validation
   ↓
Final Table (GUEST_STAY)
   ↓
Analytics & Stored Procedures
```

---

# 🚀 Step-by-Step Setup Process

# Step 1 — Clone Repository

```bash
git clone https://github.com/your-username/Hospitality-ETL-Pipeline.git

cd Hospitality-ETL-Pipeline
```

---

# Step 2 — Install Required Packages

Install Python dependencies:

```bash
pip install snowflake-connector-python pandas
```

---

# Step 3 — Create Snowflake Account

Create a free Snowflake account:

* [https://signup.snowflake.com/](https://signup.snowflake.com/)

---

# Step 4 — Configure Snowflake Credentials

Open:

```python
hospitalityusecase.py
```

Update:

```python
SNOWFLAKE_CONFIG = {
    "user":      "YOUR_USERNAME",
    "password":  "YOUR_PASSWORD",
    "account":   "YOUR_ACCOUNT",
    "warehouse": "COMPUTE_WH",
    "database":  "HOSPITALITY_DB",
    "schema":    "PUBLIC",
}
```

---

# Step 5 — Place CSV Files

Place these files in the same folder as the Python script:

```bash
Guest_Master_Hospitality.csv
Room_Master_Hospitality.csv
Checkin_Checkout_Hospitality.csv
```

---

# Step 6 — Run Snowflake SQL Script

Open Snowflake Worksheet.

Run:

```sql
snowflake code.txt
```

Run each step separately.

This will create:

* Database
* Schema
* Tables
* Stage
* Stored Procedure
* Audit Table

---

# Step 7 — Run Python ETL Pipeline

Execute:

```bash
python hospitalityusecase.py
```

---

# 🔄 ETL Flow Explained

## 1. Extract Phase

Reads CSV files using Pandas.

```python
pd.read_csv()
```

---

## 2. Validation Phase

Checks data quality issues:

* Missing IDs
* NULL dates
* Invalid values

---

## 3. Transformation Phase

Business transformations:

```python
StayDurationDays
CalcTotalAmount
GuestFullName
```

---

## 4. Load Phase

Loads data into:

* STG_GUEST_MASTER
* STG_ROOM_MASTER
* STG_CHECKIN_CHECKOUT
* GUEST_STAY

---

## 5. Audit Logging

Tracks INSERT and UPDATE operations in:

```sql
GUEST_STAY_AUDIT
```

---

# 📈 Stored Procedure

Stored Procedure:

```sql
SP_DAILY_OCCUPANCY_REVENUE
```

Returns:

* Total rooms
* Occupied rooms
* Revenue
* Occupancy %
* Average revenue per room

---

# 📌 Example Stored Procedure Call

```sql
CALL SP_DAILY_OCCUPANCY_REVENUE('2026-05-28');
```

---

# 📊 Example Analytical Queries

## Revenue by Room Type

```sql
SELECT
    RoomType,
    SUM(TotalAmount) AS Revenue
FROM GUEST_STAY
GROUP BY RoomType;
```

---

## Top Guests

```sql
SELECT
    GuestFullName,
    SUM(TotalAmount) AS Revenue
FROM GUEST_STAY
GROUP BY GuestFullName
ORDER BY Revenue DESC;
```

---

# 🔥 Key Concepts Covered

* ETL Pipeline
* Data Warehousing
* Snowflake
* SQL MERGE
* Incremental Loading
* Data Validation
* Data Transformation
* Stored Procedures
* Analytical Queries
* Audit Logging

---

# 📷 Output Example

```text
[CONNECT] Connected ✓
[DDL] Done.
[EXTRACT] Full load — 500 records loaded
[VALIDATE] All checks passed.
[TRANSFORM] 500 enriched records ready.
[LOAD] Staging tables populated.
[LOAD] 500 stays upserted.
```

---

# 🎯 Learning Outcomes

By completing this project, you will understand:

* Real-world ETL pipeline development
* Snowflake database operations
* Data cleaning and transformation
* Data warehouse concepts
* Analytical reporting
* Python + SQL integration

---

# 👨‍💻 Author

Developed for Hospitality Domain ETL and Snowflake Practice Project.

---

# 📜 License

This project is for educational and learning purposes.
