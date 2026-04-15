# app.py
from flask import Flask
import mysql.connector
import gspread
from google.oauth2.service_account import Credentials
import os, json
from datetime import datetime, date, timedelta
import logging
import pandas as pd
from shapely.geometry import Point, Polygon
from gspread_dataframe import set_with_dataframe


# --- Minimal logging setup ---
logging.getLogger('werkzeug').setLevel(logging.WARNING)
for name in ('gunicorn.error', 'gunicorn.access'):
    logging.getLogger(name).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

app = Flask(__name__)

# ---------------- Helper: Compute Zone ----------------
def get_zone(lat, lng, zones):
    if pd.notna(lat) and pd.notna(lng) and lat != "" and lng != "":
        try:
            point = Point(float(lng), float(lat))
            for z in zones:
                if z["polygon"].contains(point):
                    return z["area"]
        except (ValueError, TypeError) as e:
            logger.warning(f"Invalid lat/lng values: lat={lat}, lng={lng} → {e}")
    return None

# ---------------- Load Zones from Database ----------------
def load_zones(cursor):
    cursor.execute(
        "SELECT id, area, polygon FROM plb_city_area_polygons "
        "WHERE id NOT IN (3,4,5,6,10,17,18,9,19,20,1,11,21,22)"
    )
    zones_raw = cursor.fetchall()
    zones = []
    for z in zones_raw:
        try:
            pts = json.loads(z["polygon"])
            coords = [(float(p["lng"]), float(p["lat"])) for p in pts if p.get("lat") and p.get("lng")]
            if len(coords) < 3:
                logger.warning(f"Skipping zone {z.get('id')} — fewer than 3 valid coordinates.")
                continue
            poly = Polygon(coords)
            if not poly.is_valid:
                poly = poly.buffer(0)
            zones.append({"area": z["area"], "polygon": poly})
        except Exception as e:
            logger.error(f"Polygon load error for zone {z.get('id', 'unknown')} → {e}")
    logger.info(f"Loaded {len(zones)} zones from DB.")
    return zones

@app.route('/')
def home():
    return "Server running fine ✅"

@app.route('/update-sheet', methods=['GET', 'POST'])
def update_sheet():
    db = None
    cursor = None
    try:
        # Load credentials from environment
        db_credentials = json.loads(os.environ["DB_CREDENTIALS"])
        google_creds = json.loads(os.environ["GOOGLE_CREDS"])

        # Connect to MySQL — dictionary=True so columns are accessible by name
        db = mysql.connector.connect(
            host=db_credentials["host"],
            user=db_credentials["user"],
            password=db_credentials["password"],
            database=db_credentials["database"]
        )
        cursor = db.cursor(dictionary=True)   # ✅ FIX: was db.cursor() — returned tuples

        zones = load_zones(cursor)

        # Auth Google Sheets
        creds = Credentials.from_service_account_info(
            google_creds,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        client = gspread.Client(auth=creds)   # ✅ FIX: gspread.authorize() is deprecated
        sheet = client.open_by_key("1YuNXJ0IFguyZ0Bg5UKwvi2VskH7oYLgvA2mTEZ0-B74").worksheet("Dump Data")

        # Run full SQL query
        cursor.execute("""
        SELECT plb_history_bookings.id,
        plb_history_bookings.booking_date,
        plb_history_bookings.booking_time,
        plb_history_bookings.created_at,
        CONCAT(
            LPAD(HOUR(plb_history_bookings.booking_time), 2, '0'), ':',
            LPAD(FLOOR(MINUTE(plb_history_bookings.booking_time) / 30) * 30, 2, '0'),
            ' - ',
            LPAD(HOUR(plb_history_bookings.booking_time + INTERVAL 30 MINUTE), 2, '0'), ':',
            LPAD(FLOOR(MINUTE(plb_history_bookings.booking_time + INTERVAL 30 MINUTE) / 30) * 30, 2, '0')
        ) AS slot,
        c.mobile_number,
        CASE
            WHEN plb_history_bookings.family_member_id = 0 THEN c.gender
            ELSE plb_customer_family_members.gender
        END AS customer_gender,
        CASE
            WHEN plb_history_bookings.family_member_id = 0 THEN TIMESTAMPDIFF(YEAR, c.dob, CURDATE())
            ELSE TIMESTAMPDIFF(YEAR, plb_customer_family_members.dob, CURDATE())
        END AS customer_age,
        plb_history_bookings.booking_by,
        CONCAT(plb_manages.first_name, ' ', plb_manages.last_name) AS Created_by_name,
        CASE
            WHEN plb_history_bookings.booking_admin_type = 1 THEN 'P1 - Curelo New'
            WHEN plb_history_bookings.booking_admin_type = 2 THEN 'P2 - Curelo Repeat'
            WHEN plb_history_bookings.booking_admin_type = 3 THEN 'L1 - Lab New'
            WHEN plb_history_bookings.booking_admin_type = 4 THEN 'L2 - Lab Repeat'
            WHEN plb_history_bookings.booking_admin_type = 5 THEN 'C - Corporate Lead'
        END AS Customer_Type,
        CASE
            WHEN plb_history_bookings.booking_tracking_id = 1 THEN 'Order Placed'
            WHEN plb_history_bookings.booking_tracking_id = 2 THEN 'Phlebotomist Assigned'
            WHEN plb_history_bookings.booking_tracking_id = 3 THEN 'Phlebotomist On The Way'
            WHEN plb_history_bookings.booking_tracking_id = 4 THEN 'Phlebotomist Reached at Destination'
            WHEN plb_history_bookings.booking_tracking_id = 5 THEN 'Phlebotomist Collection Received'
            WHEN plb_history_bookings.booking_tracking_id = 6 THEN 'Phlebotomist Sample Submitted'
            WHEN plb_history_bookings.booking_tracking_id = 7 THEN 'Reports Preparing'
            WHEN plb_history_bookings.booking_tracking_id = 8 THEN 'Reports Submitted'
            WHEN plb_history_bookings.booking_tracking_id = 9 THEN 'Order Completed'
        END AS Booking_Stage,
        plb_history_bookings.status,
        plb_history_bookings.booking_status,
        plb_history_bookings.promocode,
        CASE
            WHEN plb_history_bookings.booking_by = 'customer' AND plb_promocodes.promocode_category IS NULL THEN 'Organic Lead'
            WHEN plb_history_bookings.booking_by = 'lab' AND plb_history_bookings.total_admin_commission = plb_history_bookings.fixed_admin_commission AND plb_promocodes.promocode_category IS NULL THEN 'Lab Lead'
            WHEN plb_history_bookings.booking_by = 'admin' AND plb_history_bookings.total_admin_commission = plb_history_bookings.fixed_admin_commission AND plb_promocodes.promocode_category IS NULL THEN 'Lab Lead by Admin'
            WHEN plb_history_bookings.booking_by = 'admin' AND plb_promocodes.promocode_category IS NULL THEN 'Organic Lead by Admin'
            ELSE plb_promocode_categories.name
        END AS Channel,
        plb_labs.name AS Lab_Name,
        plb_cities.name AS city_name,
        GROUP_CONCAT(DISTINCT CASE
            WHEN plb_lab_tests.title IS NULL THEN plb_tests_and_packages_masters.name
            ELSE plb_lab_tests.title
        END SEPARATOR ', ') AS tests,
        GROUP_CONCAT(DISTINCT CASE
            WHEN plb_lab_tests.title IS NULL THEN plb_tests_and_packages_masters.type
            ELSE plb_lab_tests.type
        END SEPARATOR ', ') AS tests_type,
        CONCAT(plb_phlebos.first_name, ' ', plb_phlebos.last_name) AS phlebo_name,
        plb_customer_addresses.pincode,
        plb_customer_addresses.complete_address,
        plb_customer_addresses.latitude,    
        plb_customer_addresses.longitude,   
        CASE
            WHEN plb_history_bookings.family_member_id = 0 THEN CONCAT(c.first_name, ' ', c.last_name)
            ELSE CONCAT(plb_customer_family_members.first_name, ' ', plb_customer_family_members.last_name)
        END AS customer_name,
        plb_history_bookings.booking_note,
        plb_history_bookings.total_actual_amount,
        plb_history_bookings.discount_amount,
        (plb_history_bookings.total_actual_amount - plb_history_bookings.discount_amount) AS lab_mrp,
        plb_history_bookings.promocode_discount_amount,
        plb_history_bookings.redeem_coin,
        plb_history_bookings.total_paid_amount
        FROM plb_history_bookings
        LEFT JOIN plb_history_booking_tests ON plb_history_bookings.id = plb_history_booking_tests.booking_id
        LEFT JOIN plb_promocodes ON plb_history_bookings.promocode = plb_promocodes.promocode
        LEFT JOIN plb_promocode_categories ON plb_promocodes.promocode_category = plb_promocode_categories.id
        LEFT JOIN plb_phlebos ON plb_phlebos.id = plb_history_bookings.phlebo_id
        LEFT JOIN plb_cities ON plb_cities.id = plb_history_bookings.city_id
        LEFT JOIN plb_customers AS c ON plb_history_bookings.customer_id = c.id
        LEFT JOIN plb_customer_family_members ON plb_history_bookings.family_member_id = plb_customer_family_members.id
        LEFT JOIN plb_customer_addresses ON plb_customer_addresses.id = plb_history_bookings.address_id
        LEFT JOIN plb_labs ON plb_labs.id = plb_history_bookings.lab_id
        LEFT JOIN plb_labs_branches ON plb_history_bookings.lab_branch_id = plb_labs_branches.id
        LEFT JOIN plb_lab_tests ON plb_lab_tests.id = plb_history_booking_tests.test_id
        LEFT JOIN plb_manages ON plb_manages.id = plb_history_bookings.booking_by_id
        LEFT JOIN plb_tests_and_packages_masters ON plb_lab_tests.test_and_package_id = plb_tests_and_packages_masters.id
        WHERE plb_history_bookings.booking_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 1 DAY)
        GROUP BY plb_history_bookings.id

        UNION ALL

        SELECT plb_bookings.id,
        plb_bookings.booking_date,
        plb_bookings.booking_time,
        plb_bookings.created_at,
        CONCAT(
            LPAD(HOUR(plb_bookings.booking_time), 2, '0'), ':',
            LPAD(FLOOR(MINUTE(plb_bookings.booking_time) / 30) * 30, 2, '0'),
            ' - ',
            LPAD(HOUR(plb_bookings.booking_time + INTERVAL 30 MINUTE), 2, '0'), ':',
            LPAD(FLOOR(MINUTE(plb_bookings.booking_time + INTERVAL 30 MINUTE) / 30) * 30, 2, '0')
        ) AS slot,
        c.mobile_number,
        CASE
            WHEN plb_bookings.family_member_id = 0 THEN c.gender
            ELSE plb_customer_family_members.gender
        END AS customer_gender,
        CASE
            WHEN plb_bookings.family_member_id = 0 THEN TIMESTAMPDIFF(YEAR, c.dob, CURDATE())
            ELSE TIMESTAMPDIFF(YEAR, plb_customer_family_members.dob, CURDATE())
        END AS customer_age,
        plb_bookings.booking_by,
        CONCAT(plb_manages.first_name, ' ', plb_manages.last_name) AS Created_by_name,
        CASE
            WHEN plb_bookings.booking_admin_type = 1 THEN 'P1 - Curelo New'
            WHEN plb_bookings.booking_admin_type = 2 THEN 'P2 - Curelo Repeat'
            WHEN plb_bookings.booking_admin_type = 3 THEN 'L1 - Lab New'
            WHEN plb_bookings.booking_admin_type = 4 THEN 'L2 - Lab Repeat'
            WHEN plb_bookings.booking_admin_type = 5 THEN 'C - Corporate Lead'
        END AS Customer_Type,
        CASE
            WHEN plb_bookings.booking_tracking_id = 1 THEN 'Order Placed'
            WHEN plb_bookings.booking_tracking_id = 2 THEN 'Phlebotomist Assigned'
            WHEN plb_bookings.booking_tracking_id = 3 THEN 'Phlebotomist On The Way'
            WHEN plb_bookings.booking_tracking_id = 4 THEN 'Phlebotomist Reached at Destination'
            WHEN plb_bookings.booking_tracking_id = 5 THEN 'Phlebotomist Collection Received'
            WHEN plb_bookings.booking_tracking_id = 6 THEN 'Phlebotomist Sample Submitted'
            WHEN plb_bookings.booking_tracking_id = 7 THEN 'Reports Preparing'
            WHEN plb_bookings.booking_tracking_id = 8 THEN 'Reports Submitted'
            WHEN plb_bookings.booking_tracking_id = 9 THEN 'Order Completed'
        END AS Booking_Stage,
        plb_bookings.status,
        plb_bookings.booking_status,
        plb_bookings.promocode,
        CASE
            WHEN plb_bookings.booking_by = 'customer' AND plb_promocodes.promocode_category IS NULL THEN 'Organic Lead'
            WHEN plb_bookings.booking_by = 'lab' AND plb_bookings.total_admin_commission = plb_bookings.fixed_admin_commission AND plb_promocodes.promocode_category IS NULL THEN 'Lab Lead'
            WHEN plb_bookings.booking_by = 'admin' AND plb_bookings.total_admin_commission = plb_bookings.fixed_admin_commission AND plb_promocodes.promocode_category IS NULL THEN 'Lab Lead by Admin'
            WHEN plb_bookings.booking_by = 'admin' AND plb_promocodes.promocode_category IS NULL THEN 'Organic Lead by Admin'
            ELSE plb_promocode_categories.name
        END AS Channel,
        plb_labs.name AS Lab_Name,
        plb_cities.name AS city_name,
        GROUP_CONCAT(DISTINCT CASE
            WHEN plb_lab_tests.title IS NULL THEN plb_tests_and_packages_masters.name
            ELSE plb_lab_tests.title
        END SEPARATOR ', ') AS tests,
        GROUP_CONCAT(DISTINCT CASE
            WHEN plb_lab_tests.title IS NULL THEN plb_tests_and_packages_masters.type
            ELSE plb_lab_tests.type
        END SEPARATOR ', ') AS tests_type,
        CONCAT(plb_phlebos.first_name, ' ', plb_phlebos.last_name) AS phlebo_name,
        plb_customer_addresses.pincode,
        plb_customer_addresses.complete_address,
        plb_customer_addresses.latitude,   
        plb_customer_addresses.longitude,  
        CASE
            WHEN plb_bookings.family_member_id = 0 THEN CONCAT(c.first_name, ' ', c.last_name)
            ELSE CONCAT(plb_customer_family_members.first_name, ' ', plb_customer_family_members.last_name)
        END AS customer_name,
        plb_bookings.booking_note,
        plb_bookings.total_actual_amount,
        plb_bookings.discount_amount,
        (plb_bookings.total_actual_amount - plb_bookings.discount_amount) AS lab_mrp,
        plb_bookings.promocode_discount_amount,
        plb_bookings.redeem_coin,
        plb_bookings.total_paid_amount
        FROM plb_bookings
        LEFT JOIN plb_booking_tests ON plb_bookings.id = plb_booking_tests.booking_id
        LEFT JOIN plb_promocodes ON plb_bookings.promocode = plb_promocodes.promocode
        LEFT JOIN plb_promocode_categories ON plb_promocodes.promocode_category = plb_promocode_categories.id
        LEFT JOIN plb_phlebos ON plb_phlebos.id = plb_bookings.phlebo_id
        LEFT JOIN plb_cities ON plb_cities.id = plb_bookings.city_id
        LEFT JOIN plb_customers AS c ON plb_bookings.customer_id = c.id
        LEFT JOIN plb_customer_family_members ON plb_bookings.family_member_id = plb_customer_family_members.id
        LEFT JOIN plb_customer_addresses ON plb_customer_addresses.id = plb_bookings.address_id
        LEFT JOIN plb_labs ON plb_labs.id = plb_bookings.lab_id
        LEFT JOIN plb_labs_branches ON plb_bookings.lab_branch_id = plb_labs_branches.id
        LEFT JOIN plb_lab_tests ON plb_lab_tests.id = plb_booking_tests.test_id
        LEFT JOIN plb_manages ON plb_manages.id = plb_bookings.booking_by_id
        LEFT JOIN plb_tests_and_packages_masters ON plb_lab_tests.test_and_package_id = plb_tests_and_packages_masters.id
        WHERE plb_bookings.booking_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 1 DAY)
        GROUP BY plb_bookings.id
        ORDER BY booking_date
        """)

        rows = cursor.fetchall()
        df = pd.DataFrame(rows)

        if not df.empty:
            # ---------------- Compute Zones ----------------
            df["zone"] = df.apply(
                lambda r: get_zone(r.get("latitude"), r.get("longitude"), zones),
                axis=1
            )
            df = df.fillna("").astype(str)
            sheet.clear()
            set_with_dataframe(sheet, df)
            logger.info(f"Sheet updated successfully with {len(df)} rows.")
        else:
            logger.info("No bookings found for today.")

        return "OK", 200

    except Exception as e:
        logger.exception("Error updating sheet")
        return str(e), 500

    finally:
        # ✅ FIX: Always close cursor and DB connection to prevent connection leaks
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()
            logger.info("DB connection closed.")

# ---------------- Run Flask ----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
