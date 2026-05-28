"""
=============================================================
  HOSPITALITY ETL PIPELINE — SNOWFLAKE
  Covers ALL use cases from UseCase_Hospitality.docx
=============================================================
REQUIREMENTS:
    pip install snowflake-connector-python pandas
SETUP:
  1. Fill in your Snowflake credentials in SNOWFLAKE_CONFIG.
  2. Place the three CSV files in the same folder as this script.
  3. Run:  python hospitality_snowflake_etl.py
=============================================================
"""

import os
import math
import pandas as pd
import snowflake.connector
from snowflake.connector import DictCursor
from datetime import datetime, date

# ─────────────────────────────────────────────────────────────
# 1.  CONFIGURATION
# ─────────────────────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "user":      "Navaneethan",
    "password":  "922522205101@It",
    "account":   "sy62891.ap-southeast-7.aws",
    "warehouse": "COMPUTE_WH",
    "database":  "HOSPITALITY_DB",
    "schema":    "PUBLIC",
}

CSV_DIR     = os.path.dirname(os.path.abspath(__file__))
CHECKIN_CSV = os.path.join(CSV_DIR, "Checkin_Checkout_Hospitality.csv")
GUEST_CSV   = os.path.join(CSV_DIR, "Guest_Master_Hospitality.csv")
ROOM_CSV    = os.path.join(CSV_DIR, "Room_Master_Hospitality.csv")

LAST_RUN_TIME = None  # Set to datetime(YYYY,MM,DD) for incremental loads


# ─────────────────────────────────────────────────────────────
# 2.  SNOWFLAKE CONNECTION
# ─────────────────────────────────────────────────────────────
def get_connection():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        account=SNOWFLAKE_CONFIG["account"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
    )
    return conn


# ─────────────────────────────────────────────────────────────
# 3.  DDL
# ─────────────────────────────────────────────────────────────
DDL_STATEMENTS = [
    f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}",
    f"USE DATABASE {SNOWFLAKE_CONFIG['database']}",
    f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONFIG['schema']}",
    f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}",

    """
    CREATE TABLE IF NOT EXISTS STG_GUEST_MASTER (
        GuestID          NUMBER,
        FirstName        VARCHAR(100),
        LastName         VARCHAR(100),
        Gender           VARCHAR(10),
        DateOfBirth      DATE,
        Email            VARCHAR(200),
        PhoneNumber      VARCHAR(20),
        AddressLine1     VARCHAR(200),
        AddressLine2     VARCHAR(200),
        City             VARCHAR(100),
        State            VARCHAR(100),
        Country          VARCHAR(100),
        IDProofType      VARCHAR(50),
        IDProofNumber    VARCHAR(50),
        RegistrationDate DATE,
        LOAD_TIMESTAMP   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS STG_ROOM_MASTER (
        RoomID          NUMBER,
        RoomType        VARCHAR(50),
        FloorNumber     NUMBER,
        BedType         VARCHAR(50),
        BaseRate        NUMBER(10,2),
        RoomStatus      VARCHAR(20),
        MaxOccupancy    NUMBER,
        Amenities       VARCHAR(500),
        LastCleanedDate DATE,
        LOAD_TIMESTAMP  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS STG_CHECKIN_CHECKOUT (
        StayID           NUMBER,
        GuestID          NUMBER,
        RoomID           NUMBER,
        CheckinDateTime  TIMESTAMP,
        CheckoutDateTime TIMESTAMP,
        BookingSource    VARCHAR(50),
        NumberOfGuests   NUMBER,
        RoomRate         NUMBER(10,2),
        ExtraCharges     NUMBER(10,2),
        DiscountAmount   NUMBER(10,2),
        TotalAmount      NUMBER(10,2),
        PaymentMode      VARCHAR(50),
        Status           VARCHAR(20),
        LastUpdated      DATE,
        LOAD_TIMESTAMP   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS GUEST_STAY (
        StayID              NUMBER PRIMARY KEY,
        GuestID             NUMBER,
        GuestFullName       VARCHAR(200),
        Gender              VARCHAR(10),
        Email               VARCHAR(200),
        City                VARCHAR(100),
        Country             VARCHAR(100),
        RoomID              NUMBER,
        RoomType            VARCHAR(50),
        BedType             VARCHAR(50),
        FloorNumber         NUMBER,
        Amenities           VARCHAR(500),
        CheckinDateTime     TIMESTAMP,
        CheckoutDateTime    TIMESTAMP,
        StayDurationHours   NUMBER(10,2),
        StayDurationDays    NUMBER(10,2),
        BookingSource       VARCHAR(50),
        NumberOfGuests      NUMBER,
        RoomRate            NUMBER(10,2),
        ExtraCharges        NUMBER(10,2),
        DiscountAmount      NUMBER(10,2),
        TotalAmount         NUMBER(10,2),
        PaymentMode         VARCHAR(50),
        Status              VARCHAR(20),
        LastUpdated         DATE,
        ETL_LOAD_TIMESTAMP  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS GUEST_STAY_AUDIT (
        AuditID         NUMBER AUTOINCREMENT PRIMARY KEY,
        StayID          NUMBER,
        OperationType   VARCHAR(10),
        ChangedAt       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        ChangedBy       VARCHAR(100) DEFAULT CURRENT_USER(),
        OldStatus       VARCHAR(20),
        NewStatus       VARCHAR(20),
        OldTotalAmount  NUMBER(10,2),
        NewTotalAmount  NUMBER(10,2)
    )
    """,
]


def run_ddl(conn):
    print("\n[DDL] Creating database, schema, and tables ...")
    cur = conn.cursor()
    for stmt in DDL_STATEMENTS:
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    cur.close()
    print("[DDL] Done.")


# ─────────────────────────────────────────────────────────────
# 4.  EXTRACT
# ─────────────────────────────────────────────────────────────
def extract_data():
    print("\n[EXTRACT] Reading CSV files ...")

    gdf = pd.read_csv(GUEST_CSV)
    gdf.columns = gdf.columns.str.strip()
    gdf["DateOfBirth"]      = pd.to_datetime(gdf["DateOfBirth"],      errors="coerce")
    gdf["RegistrationDate"] = pd.to_datetime(gdf["RegistrationDate"], errors="coerce")
    gdf["PhoneNumber"]      = gdf["PhoneNumber"].astype(str).str.strip()
    gdf.fillna("", inplace=True)

    rdf = pd.read_csv(ROOM_CSV)
    rdf.columns = rdf.columns.str.strip()
    rdf["LastCleanedDate"] = pd.to_datetime(rdf["LastCleanedDate"], errors="coerce")
    rdf.fillna("", inplace=True)

    cdf = pd.read_csv(CHECKIN_CSV)
    cdf.columns = cdf.columns.str.strip()
    cdf["CheckinDateTime"]  = pd.to_datetime(cdf["CheckinDateTime"],  errors="coerce")
    cdf["CheckoutDateTime"] = pd.to_datetime(cdf["CheckoutDateTime"], errors="coerce")
    cdf["LastUpdated"]      = pd.to_datetime(cdf["LastUpdated"],      errors="coerce")

    if LAST_RUN_TIME:
        cdf = cdf[cdf["LastUpdated"] > pd.Timestamp(LAST_RUN_TIME)]
        print(f"[EXTRACT] Incremental filter — {len(cdf)} records since {LAST_RUN_TIME}")
    else:
        print(f"[EXTRACT] Full load — {len(cdf)} check-in records loaded")

    print(f"[EXTRACT] Guests: {len(gdf)} | Rooms: {len(rdf)} | Stays: {len(cdf)}")
    return gdf, rdf, cdf


# ─────────────────────────────────────────────────────────────
# 5.  VALIDATE
# ─────────────────────────────────────────────────────────────
def validate_data(gdf, rdf, cdf):
    print("\n[VALIDATE] Checking data quality ...")
    issues = []

    missing_guests = set(cdf["GuestID"]) - set(gdf["GuestID"])
    if missing_guests:
        issues.append(f"  WARN: {len(missing_guests)} GuestID(s) not in Guest Master: {missing_guests}")

    missing_rooms = set(cdf["RoomID"]) - set(rdf["RoomID"])
    if missing_rooms:
        issues.append(f"  WARN: {len(missing_rooms)} RoomID(s) not in Room Master: {missing_rooms}")

    null_checkin = cdf["CheckinDateTime"].isna().sum()
    if null_checkin:
        issues.append(f"  WARN: {null_checkin} rows with NULL CheckinDateTime")

    both_present = cdf.dropna(subset=["CheckinDateTime", "CheckoutDateTime"])
    bad_dates = both_present[both_present["CheckoutDateTime"] < both_present["CheckinDateTime"]]
    if not bad_dates.empty:
        issues.append(f"  WARN: {len(bad_dates)} rows where CheckoutDateTime < CheckinDateTime")

    neg_amounts = cdf[cdf["TotalAmount"] < 0]
    if not neg_amounts.empty:
        issues.append(f"  WARN: {len(neg_amounts)} rows with negative TotalAmount")

    active = cdf["CheckoutDateTime"].isna().sum()
    if active:
        print(f"[VALIDATE] INFO: {active} active (CheckedIn) stays — duration calculated to now")

    if issues:
        print("[VALIDATE] Issues found:")
        for i in issues:
            print(i)
    else:
        print("[VALIDATE] All checks passed.")

    return issues


# ─────────────────────────────────────────────────────────────
# 6.  TRANSFORM
# ─────────────────────────────────────────────────────────────
def safe_ceil(x):
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return 1
        return max(1, math.ceil(x))
    except Exception:
        return 1


def transform_data(gdf, rdf, cdf):
    print("\n[TRANSFORM] Enriching data ...")
    now = pd.Timestamp(datetime.now())

    gdf["GuestFullName"] = gdf["FirstName"].str.strip() + " " + gdf["LastName"].str.strip()
    guest_cols = ["GuestID", "GuestFullName", "Gender", "Email", "City", "Country"]
    merged = cdf.merge(gdf[guest_cols], on="GuestID", how="left")

    room_cols = ["RoomID", "RoomType", "BedType", "FloorNumber", "Amenities"]
    merged = merged.merge(rdf[room_cols], on="RoomID", how="left")

    # Active stays: fill missing checkout with current time for duration calc
    effective_checkout = merged["CheckoutDateTime"].fillna(now)

    merged["StayDurationHours"] = (
        (effective_checkout - merged["CheckinDateTime"])
        .dt.total_seconds() / 3600
    ).round(2)
    merged["StayDurationHours"] = merged["StayDurationHours"].fillna(0.0)
    merged["StayDurationDays"]  = (merged["StayDurationHours"] / 24).round(2)

    merged["CalcTotalAmount"] = (
        merged["RoomRate"] * merged["StayDurationDays"].apply(safe_ceil)
        + merged["ExtraCharges"]
        - merged["DiscountAmount"]
    ).round(2)

    discrepancies = merged[abs(merged["TotalAmount"] - merged["CalcTotalAmount"]) > 1]
    if not discrepancies.empty:
        print(f"  [WARN] {len(discrepancies)} rows have TotalAmount discrepancy — using source value")

    merged["GuestFullName"] = merged["GuestFullName"].fillna("Unknown")
    merged["RoomType"]      = merged["RoomType"].fillna("Unknown")
    merged["BedType"]       = merged["BedType"].fillna("Unknown")
    merged["Amenities"]     = merged["Amenities"].fillna("")

    print(f"[TRANSFORM] {len(merged)} enriched records ready.")
    return merged


# ─────────────────────────────────────────────────────────────
# 7.  LOAD — Staging
# ─────────────────────────────────────────────────────────────
def _safe_val(v):
    if isinstance(v, float) and math.isnan(v):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if hasattr(v, "item"):
        return v.item()
    return v


def load_staging(conn, gdf, rdf, cdf):
    print("\n[LOAD] Truncating and reloading staging tables ...")
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE STG_GUEST_MASTER")
    for _, row in gdf.iterrows():
        cur.execute("""
            INSERT INTO STG_GUEST_MASTER
            (GuestID,FirstName,LastName,Gender,DateOfBirth,Email,PhoneNumber,
             AddressLine1,AddressLine2,City,State,Country,IDProofType,IDProofNumber,RegistrationDate)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [_safe_val(row.get(c)) for c in [
            "GuestID","FirstName","LastName","Gender","DateOfBirth","Email","PhoneNumber",
            "AddressLine1","AddressLine2","City","State","Country","IDProofType","IDProofNumber","RegistrationDate"
        ]])

    cur.execute("TRUNCATE TABLE STG_ROOM_MASTER")
    for _, row in rdf.iterrows():
        cur.execute("""
            INSERT INTO STG_ROOM_MASTER
            (RoomID,RoomType,FloorNumber,BedType,BaseRate,RoomStatus,MaxOccupancy,Amenities,LastCleanedDate)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [_safe_val(row.get(c)) for c in [
            "RoomID","RoomType","FloorNumber","BedType","BaseRate","RoomStatus","MaxOccupancy","Amenities","LastCleanedDate"
        ]])

    if LAST_RUN_TIME is None:
        cur.execute("TRUNCATE TABLE STG_CHECKIN_CHECKOUT")
    for _, row in cdf.iterrows():
        cur.execute("""
            INSERT INTO STG_CHECKIN_CHECKOUT
            (StayID,GuestID,RoomID,CheckinDateTime,CheckoutDateTime,BookingSource,
             NumberOfGuests,RoomRate,ExtraCharges,DiscountAmount,TotalAmount,PaymentMode,Status,LastUpdated)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [_safe_val(row.get(c)) for c in [
            "StayID","GuestID","RoomID","CheckinDateTime","CheckoutDateTime","BookingSource",
            "NumberOfGuests","RoomRate","ExtraCharges","DiscountAmount","TotalAmount","PaymentMode","Status","LastUpdated"
        ]])

    cur.close()
    print("[LOAD] Staging tables populated.")


# ─────────────────────────────────────────────────────────────
# 8.  LOAD — GUEST_STAY + Audit
# ─────────────────────────────────────────────────────────────
def load_guest_stay(conn, merged_df):
    print("\n[LOAD] Upserting into GUEST_STAY ...")
    cur = conn.cursor()
    upsert_count = 0
    audit_count  = 0

    for _, row in merged_df.iterrows():
        stay_id = _safe_val(row["StayID"])

        cur.execute("SELECT Status, TotalAmount FROM GUEST_STAY WHERE StayID = %s", (stay_id,))
        existing = cur.fetchone()

        cur.execute("""
            MERGE INTO GUEST_STAY AS tgt
            USING (SELECT %s AS StayID) AS src ON (tgt.StayID = src.StayID)
            WHEN MATCHED THEN UPDATE SET
                GuestID            = %s,
                GuestFullName      = %s,
                Gender             = %s,
                Email              = %s,
                City               = %s,
                Country            = %s,
                RoomID             = %s,
                RoomType           = %s,
                BedType            = %s,
                FloorNumber        = %s,
                Amenities          = %s,
                CheckinDateTime    = %s,
                CheckoutDateTime   = %s,
                StayDurationHours  = %s,
                StayDurationDays   = %s,
                BookingSource      = %s,
                NumberOfGuests     = %s,
                RoomRate           = %s,
                ExtraCharges       = %s,
                DiscountAmount     = %s,
                TotalAmount        = %s,
                PaymentMode        = %s,
                Status             = %s,
                LastUpdated        = %s,
                ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                StayID,GuestID,GuestFullName,Gender,Email,City,Country,
                RoomID,RoomType,BedType,FloorNumber,Amenities,
                CheckinDateTime,CheckoutDateTime,StayDurationHours,StayDurationDays,
                BookingSource,NumberOfGuests,RoomRate,ExtraCharges,DiscountAmount,
                TotalAmount,PaymentMode,Status,LastUpdated
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s
            )
        """, [
            stay_id,
            _safe_val(row["GuestID"]),       _safe_val(row["GuestFullName"]),
            _safe_val(row["Gender"]),         _safe_val(row["Email"]),
            _safe_val(row["City"]),           _safe_val(row["Country"]),
            _safe_val(row["RoomID"]),         _safe_val(row["RoomType"]),
            _safe_val(row["BedType"]),        _safe_val(row["FloorNumber"]),
            _safe_val(row["Amenities"]),
            _safe_val(row["CheckinDateTime"]),  _safe_val(row["CheckoutDateTime"]),
            _safe_val(row["StayDurationHours"]),_safe_val(row["StayDurationDays"]),
            _safe_val(row["BookingSource"]),    _safe_val(row["NumberOfGuests"]),
            _safe_val(row["RoomRate"]),         _safe_val(row["ExtraCharges"]),
            _safe_val(row["DiscountAmount"]),   _safe_val(row["TotalAmount"]),
            _safe_val(row["PaymentMode"]),      _safe_val(row["Status"]),
            _safe_val(row["LastUpdated"]),
            # INSERT values
            stay_id,
            _safe_val(row["GuestID"]),       _safe_val(row["GuestFullName"]),
            _safe_val(row["Gender"]),         _safe_val(row["Email"]),
            _safe_val(row["City"]),           _safe_val(row["Country"]),
            _safe_val(row["RoomID"]),         _safe_val(row["RoomType"]),
            _safe_val(row["BedType"]),        _safe_val(row["FloorNumber"]),
            _safe_val(row["Amenities"]),
            _safe_val(row["CheckinDateTime"]),  _safe_val(row["CheckoutDateTime"]),
            _safe_val(row["StayDurationHours"]),_safe_val(row["StayDurationDays"]),
            _safe_val(row["BookingSource"]),    _safe_val(row["NumberOfGuests"]),
            _safe_val(row["RoomRate"]),         _safe_val(row["ExtraCharges"]),
            _safe_val(row["DiscountAmount"]),   _safe_val(row["TotalAmount"]),
            _safe_val(row["PaymentMode"]),      _safe_val(row["Status"]),
            _safe_val(row["LastUpdated"]),
        ])
        upsert_count += 1

        new_status = _safe_val(row["Status"])
        new_amount = _safe_val(row["TotalAmount"])

        if existing:
            old_status, old_amount = existing
            if old_status != new_status or old_amount != new_amount:
                cur.execute("""
                    INSERT INTO GUEST_STAY_AUDIT
                    (StayID,OperationType,OldStatus,NewStatus,OldTotalAmount,NewTotalAmount)
                    VALUES (%s,'UPDATE',%s,%s,%s,%s)
                """, (stay_id, old_status, new_status, old_amount, new_amount))
                audit_count += 1
        else:
            cur.execute("""
                INSERT INTO GUEST_STAY_AUDIT
                (StayID,OperationType,OldStatus,NewStatus,OldTotalAmount,NewTotalAmount)
                VALUES (%s,'INSERT',NULL,%s,NULL,%s)
            """, (stay_id, new_status, new_amount))
            audit_count += 1

    conn.commit()
    cur.close()
    print(f"[LOAD] {upsert_count} stays upserted | {audit_count} audit records written.")


# ─────────────────────────────────────────────────────────────
# 9.  STORED PROCEDURE
#     FIX: JS stored procedures in Snowflake do NOT support
#     Python-style bind variables (:1). The date is injected
#     as a literal string inside the SQL text instead.
# ─────────────────────────────────────────────────────────────
STORED_PROCEDURE_SQL = """
CREATE OR REPLACE PROCEDURE SP_DAILY_OCCUPANCY_REVENUE(RUN_DATE VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var result = {};

    // Total rooms in the property
    var r1 = snowflake.execute({ sqlText: "SELECT COUNT(*) FROM STG_ROOM_MASTER" });
    r1.next();
    result.TotalRoomsAvailable = r1.getColumnValue(1);

    // Rooms occupied on RUN_DATE (string literal injected safely)
    var occupiedSQL =
        "SELECT COUNT(DISTINCT RoomID) FROM GUEST_STAY " +
        "WHERE DATE(CheckinDateTime) <= '" + RUN_DATE + "' " +
        "AND (CheckoutDateTime IS NULL OR DATE(CheckoutDateTime) >= '" + RUN_DATE + "') " +
        "AND Status IN ('CheckedIn','CheckedOut')";
    var r2 = snowflake.execute({ sqlText: occupiedSQL });
    r2.next();
    result.RoomsOccupied = r2.getColumnValue(1);

    result.OccupancyRatePct = result.TotalRoomsAvailable > 0
        ? Math.round((result.RoomsOccupied / result.TotalRoomsAvailable) * 10000) / 100
        : 0;

    // Revenue and check-ins for RUN_DATE
    var revenueSQL =
        "SELECT COALESCE(SUM(TotalAmount),0), COUNT(*) FROM GUEST_STAY " +
        "WHERE DATE(CheckinDateTime) = '" + RUN_DATE + "'";
    var r3 = snowflake.execute({ sqlText: revenueSQL });
    r3.next();
    result.TotalRevenue  = r3.getColumnValue(1);
    result.CheckInsToday = r3.getColumnValue(2);

    result.AvgRevenuePerRoom = result.RoomsOccupied > 0
        ? Math.round((result.TotalRevenue / result.RoomsOccupied) * 100) / 100
        : 0;

    return result;
$$;
"""


def create_stored_procedure(conn):
    print("\n[DDL] Creating stored procedure SP_DAILY_OCCUPANCY_REVENUE ...")
    conn.cursor().execute(STORED_PROCEDURE_SQL)
    print("[DDL] Stored procedure created.")


def call_stored_procedure(conn, run_date: date):
    # Pass date as a plain VARCHAR string — avoids JS bind-type error
    date_str = run_date.strftime("%Y-%m-%d")
    print(f"\n[PROC] Calling SP_DAILY_OCCUPANCY_REVENUE('{date_str}') ...")
    cur = conn.cursor()
    cur.execute(f"CALL SP_DAILY_OCCUPANCY_REVENUE('{date_str}')")
    result = cur.fetchone()[0]
    cur.close()
    return result


# ─────────────────────────────────────────────────────────────
# 10. ANALYTICAL USE CASES
# ─────────────────────────────────────────────────────────────
def run_analytical_use_cases(conn):
    cur = conn.cursor(DictCursor)
    print("\n" + "=" * 60)
    print("  ANALYTICAL USE CASES")
    print("=" * 60)

    # Use Case 1: Daily Occupancy & Revenue Summary
    print("\n-- USE CASE 1: Daily Occupancy & Revenue Summary --")
    cur.execute("""
        SELECT
            (SELECT COUNT(*) FROM STG_ROOM_MASTER)
                                                        AS TOTAL_ROOMS_AVAILABLE,
            COUNT(DISTINCT gs.RoomID)                   AS ROOMS_OCCUPIED_TODAY,
            ROUND(COUNT(DISTINCT gs.RoomID) * 100.0 /
                  NULLIF((SELECT COUNT(*) FROM STG_ROOM_MASTER), 0), 2)
                                                        AS OCCUPANCY_RATE_PCT,
            COALESCE(SUM(gs.TotalAmount), 0)            AS TOTAL_REVENUE_GENERATED,
            ROUND(COALESCE(SUM(gs.TotalAmount), 0) /
                  NULLIF(COUNT(DISTINCT gs.RoomID), 0), 2)
                                                        AS AVG_REVENUE_PER_OCCUPIED_ROOM
        FROM GUEST_STAY gs
        WHERE DATE(gs.CheckinDateTime) = CURRENT_DATE()
    """)
    for row in cur.fetchall():
        for k, v in row.items():
            print(f"  {k:<45} {v}")

    # Use Case 2: Room-Type Wise Performance
    print("\n-- USE CASE 2: Room-Type Wise Performance Analysis --")
    cur.execute("""
        SELECT
            gs.RoomType,
            COUNT(*)                                AS TOTAL_BOOKINGS,
            COUNT(DISTINCT gs.RoomID)               AS ROOMS_USED,
            COALESCE(SUM(gs.TotalAmount), 0)        AS REVENUE,
            ROUND(COALESCE(AVG(gs.RoomRate), 0), 2) AS ADR_AVG_DAILY_RATE
        FROM GUEST_STAY gs
        GROUP BY gs.RoomType
        ORDER BY REVENUE DESC
    """)
    rows = cur.fetchall()
    if rows:
        header = list(rows[0].keys())
        print("  " + " | ".join(f"{h:<22}" for h in header))
        print("  " + "-" * 90)
        for row in rows:
            print("  " + " | ".join(f"{str(v):<22}" for v in row.values()))

    # Use Case 3: Guest Stay Duration Analysis
    print("\n-- USE CASE 3: Guest Stay Duration Analysis --")
    cur.execute("""
        SELECT
            ROUND(AVG(StayDurationDays), 2)  AS AVG_LENGTH_OF_STAY_DAYS,
            ROUND(MIN(StayDurationDays), 2)  AS MIN_STAY_DURATION_DAYS,
            ROUND(MAX(StayDurationDays), 2)  AS MAX_STAY_DURATION_DAYS,
            SUM(CEIL(StayDurationDays))       AS TOTAL_GUEST_NIGHTS
        FROM GUEST_STAY
        WHERE StayDurationDays IS NOT NULL AND StayDurationDays > 0
    """)
    for row in cur.fetchall():
        for k, v in row.items():
            print(f"  {k:<45} {v}")

    # Use Case 4: High-Value Guest Identification
    print("\n-- USE CASE 4: High-Value Guest Identification --")
    cur.execute("""
        SELECT
            gs.GuestID,
            gs.GuestFullName,
            SUM(gs.TotalAmount)           AS TOTAL_REVENUE_PER_GUEST,
            COUNT(gs.StayID)              AS NUMBER_OF_VISITS,
            ROUND(AVG(gs.TotalAmount), 2) AS AVG_SPEND_PER_STAY
        FROM GUEST_STAY gs
        GROUP BY gs.GuestID, gs.GuestFullName
        ORDER BY TOTAL_REVENUE_PER_GUEST DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        header = list(rows[0].keys())
        print("  " + " | ".join(f"{h:<25}" for h in header))
        print("  " + "-" * 100)
        for row in rows:
            print("  " + " | ".join(f"{str(v):<25}" for v in row.values()))

    cur.close()


# ─────────────────────────────────────────────────────────────
# 11. MAIN
# ─────────────────────────────────────────────────────────────
def main():
    start = datetime.now()
    print("=" * 60)
    print("  HOSPITALITY ETL PIPELINE START")
    print(f"  Run time: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    print("\n[CONNECT] Connecting to Snowflake ...")
    conn = get_connection()
    print("[CONNECT] Connected ✓")

    try:
        run_ddl(conn)
        create_stored_procedure(conn)

        gdf, rdf, cdf = extract_data()
        issues = validate_data(gdf, rdf, cdf)

        if issues:
            valid_guest_ids = set(gdf["GuestID"])
            valid_room_ids  = set(rdf["RoomID"])
            original_len = len(cdf)
            cdf = cdf[
                cdf["GuestID"].isin(valid_guest_ids) &
                cdf["RoomID"].isin(valid_room_ids) &
                cdf["CheckinDateTime"].notna() &
                (cdf["TotalAmount"] >= 0)
            ]
            dropped = original_len - len(cdf)
            if dropped:
                print(f"[VALIDATE] Dropped {dropped} bad rows; {len(cdf)} remain.")

        merged = transform_data(gdf, rdf, cdf)
        load_staging(conn, gdf, rdf, cdf)
        load_guest_stay(conn, merged)

        sp_result = call_stored_procedure(conn, date.today())
        print("\n[PROC] Daily KPIs from stored procedure:")
        if isinstance(sp_result, dict):
            for k, v in sp_result.items():
                print(f"  {k:<45} {v}")
        else:
            print(f"  {sp_result}")

        run_analytical_use_cases(conn)

    except Exception as exc:
        print(f"\n[ERROR] Pipeline failed: {exc}")
        raise

    finally:
        conn.close()
        elapsed = (datetime.now() - start).total_seconds()
        print(f"\n{'=' * 60}")
        print(f"  PIPELINE COMPLETE  ({elapsed:.1f}s)")
        print("=" * 60)


if __name__ == "__main__":
    main()