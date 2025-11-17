# app.py
from flask import Flask
import mysql.connector
import gspread
from google.oauth2.service_account import Credentials
import os, json
from datetime import datetime, date, timedelta
import logging

# --- Minimal logging setup ---
# Raise werkzeug (Flask dev server) and gunicorn access log levels to WARNING so they don't flood stdout.
logging.getLogger('werkzeug').setLevel(logging.WARNING)
for name in ('gunicorn.error', 'gunicorn.access'):
    logging.getLogger(name).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # change to DEBUG locally if needed

app = Flask(__name__)

@app.route('/')
def home():
    return "Server running fine ✅"

@app.route('/update-sheet', methods=['GET', 'POST'])
def update_sheet():
    try:
        # Load credentials from environment
        db_credentials = json.loads(os.environ["DB_CREDENTIALS"])
        google_creds = json.loads(os.environ["GOOGLE_CREDS"])

        # Connect to MySQL
        db = mysql.connector.connect(
            host=db_credentials["host"],
            user=db_credentials["user"],
            password=db_credentials["password"],
            database=db_credentials["database"]
        )
        cursor = db.cursor()

        # Auth Google Sheets
        creds = Credentials.from_service_account_info(
            google_creds,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key("1YuNXJ0IFguyZ0Bg5UKwvi2VskH7oYLgvA2mTEZ0-B74").worksheet("Dump Data")

        # Run full SQL query (kept as-is)
        cursor.execute("""
        select plb_history_bookings.id,
        plb_history_bookings.booking_date,
        plb_history_bookings.booking_time,
        plb_history_bookings.created_at,
        case
          when plb_history_bookings.family_member_id = 0 then concat(c.first_name,' ',c.last_name)
          else concat(plb_customer_family_members.first_name,' ',plb_customer_family_members.last_name)
          end as customer_name,
          c.mobile_number,
          case
          when plb_history_bookings.family_member_id = 0 then c.gender
          else plb_customer_family_members.gender
          end as customer_gender,
          case
          when plb_history_bookings.family_member_id = 0 then c.dob
          else plb_customer_family_members.dob
          end as customer_dob,
        plb_history_bookings.booking_by,
        concat(plb_manages.first_name,' ',plb_manages.last_name) as Created_by_name,
        case when plb_history_bookings.booking_admin_type = 1 then 'P1 - Curelo New'
         when plb_history_bookings.booking_admin_type = 2 then 'P2 - Curelo Repeat'
         when plb_history_bookings.booking_admin_type = 3 then 'L1 - Lab New'
         when plb_history_bookings.booking_admin_type = 4 then 'L2 - Lab Repeat'
         when plb_history_bookings.booking_admin_type = 5 then 'C - Corporate Lead'
        end as Customer_Type,
        case
        when plb_history_bookings.booking_tracking_id = 1 then 'Order Placed'
        when plb_history_bookings.booking_tracking_id = 2 then 'Phlebotomist Assigned'
        when plb_history_bookings.booking_tracking_id = 3 then 'Phlebotomist On The Way'
        when plb_history_bookings.booking_tracking_id = 4 then 'Phlebotomist Reached at Destination'
        when plb_history_bookings.booking_tracking_id = 5 then 'Phlebotomist Collection Received'
        when plb_history_bookings.booking_tracking_id = 6 then 'Phlebotomist Sample Submitted'
        when plb_history_bookings.booking_tracking_id = 7 then 'Reports Preparing'
        when plb_history_bookings.booking_tracking_id = 8 then 'Reports Submitted'
        when plb_history_bookings.booking_tracking_id = 9 then 'Order Completed'
        end as Booking_Stage,
        plb_history_bookings.status,
        plb_history_bookings.booking_status,                           
        plb_history_bookings.promocode,
        case
           when plb_history_bookings.booking_by ='customer' and plb_promocodes.promocode_category is null then 'Organic Lead'
           when plb_history_bookings.booking_by ='lab' and plb_history_bookings.total_admin_commission = plb_history_bookings.fixed_admin_commission and plb_promocodes.promocode_category is null then 'Lab Lead'
           when plb_history_bookings.booking_by = 'admin' and plb_history_bookings.total_admin_commission = plb_history_bookings.fixed_admin_commission and plb_promocodes.promocode_category is null then 'Lab Lead by Admin' 
           when plb_history_bookings.booking_by ='admin' and plb_promocodes.promocode_category is null then 'Organic Lead by Admin'
           else plb_promocode_categories.name
          end as Channel,
        plb_labs.name AS Lab_Name,
        plb_cities.name AS city_name,
        group_concat(distinct case 
          when plb_lab_tests.title is null then plb_tests_and_packages_masters.name
          else plb_lab_tests.title
          end SEPARATOR ', ') as tests,
          group_concat(distinct case 
          when plb_lab_tests.title is null then plb_tests_and_packages_masters.type
          else plb_lab_tests.type
          end SEPARATOR ', ') as tests_type,
        concat(plb_phlebos.first_name,' ',plb_phlebos.last_name) as phlebo_name,
          plb_customer_addresses.pincode,
        plb_history_bookings.total_actual_amount,
        plb_history_bookings.discount_amount,
        (plb_history_bookings.total_actual_amount-plb_history_bookings.discount_amount) as lab_mrp,
          plb_history_bookings.promocode_discount_amount,
          plb_history_bookings.redeem_coin,
          plb_history_bookings.total_paid_amount                     
          FROM plb_history_bookings
        LEFT Join plb_history_booking_tests on plb_history_bookings.id = plb_history_booking_tests.booking_id
        LEFT JOIN plb_promocodes ON plb_history_bookings.promocode = plb_promocodes.promocode
        LEFT JOIN plb_promocode_categories ON plb_promocodes.promocode_category = plb_promocode_categories.id
        LEFT JOIN plb_phlebos ON plb_phlebos.id = plb_history_bookings.phlebo_id
        LEFT JOIN plb_cities ON plb_cities.id = plb_history_bookings.city_id
        LEFT JOIN plb_customers AS c ON plb_history_bookings.customer_id = c.id 
        left join plb_customer_family_members on plb_history_bookings.family_member_id = plb_customer_family_members.id
        LEFT JOIN plb_customer_addresses ON plb_customer_addresses.id = plb_history_bookings.address_id
        LEFT JOIN plb_labs ON plb_labs.id = plb_history_bookings.lab_id
        LEFT JOIN plb_labs_branches ON plb_history_bookings.lab_branch_id = plb_labs_branches.id
        LEFT JOIN plb_lab_tests ON plb_lab_tests.id = plb_history_booking_tests.test_id
        left join plb_manages on plb_manages.id = plb_history_bookings.booking_by_id
        LEFT JOIN plb_tests_and_packages_masters on plb_lab_tests.test_and_package_id = plb_tests_and_packages_masters.id
        where  plb_history_bookings.booking_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 1 DAY)
        group by plb_history_bookings.id
        union all
        select plb_bookings.id,
        plb_bookings.booking_date, 
        plb_bookings.booking_time,                      
        plb_bookings.created_at,
        case
          when plb_bookings.family_member_id = 0 then concat(c.first_name,' ',c.last_name)
          else concat(plb_customer_family_members.first_name,' ',plb_customer_family_members.last_name)
          end as customer_name,
          c.mobile_number,
          case
          when plb_bookings.family_member_id = 0 then c.gender
          else plb_customer_family_members.gender
          end as customer_gender,
          case
          when plb_bookings.family_member_id = 0 then c.dob
          else plb_customer_family_members.dob
          end as customer_dob,
        plb_bookings.booking_by,
        concat(plb_manages.first_name,' ',plb_manages.last_name) as Created_by_name,
        case when plb_bookings.booking_admin_type = 1 then 'P1 - Curelo New'
         when plb_bookings.booking_admin_type = 2 then 'P2 - Curelo Repeat'
         when plb_bookings.booking_admin_type = 3 then 'L1 - Lab New'
         when plb_bookings.booking_admin_type = 4 then 'L2 - Lab Repeat'
         when plb_bookings.booking_admin_type = 5 then 'C - Corporate Lead'
        end as Customer_Type,
        case
        when plb_bookings.booking_tracking_id = 1 then 'Order Placed'
        when plb_bookings.booking_tracking_id = 2 then 'Phlebotomist Assigned'
        when plb_bookings.booking_tracking_id = 3 then 'Phlebotomist On The Way'
        when plb_bookings.booking_tracking_id = 4 then 'Phlebotomist Reached at Destination'
        when plb_bookings.booking_tracking_id = 5 then 'Phlebotomist Collection Received'
        when plb_bookings.booking_tracking_id = 6 then 'Phlebotomist Sample Submitted'
        when plb_bookings.booking_tracking_id = 7 then 'Reports Preparing'
        when plb_bookings.booking_tracking_id = 8 then 'Reports Submitted'
        when plb_bookings.booking_tracking_id = 9 then 'Order Completed'
        end as Booking_Stage,
        plb_bookings.status,
        plb_bookings.booking_status,                      
        plb_bookings.promocode,
        case
           when plb_bookings.booking_by ='customer' and plb_promocodes.promocode_category is null then 'Organic Lead'
           when plb_bookings.booking_by ='lab' and plb_bookings.total_admin_commission = plb_bookings.fixed_admin_commission and plb_promocodes.promocode_category is null then 'Lab Lead'
           when plb_bookings.booking_by = 'admin' and plb_bookings.total_admin_commission = plb_bookings.fixed_admin_commission and plb_promocodes.promocode_category is null then 'Lab Lead by Admin' 
           when plb_bookings.booking_by ='admin' and plb_promocodes.promocode_category is null then 'Organic Lead by Admin'
           else plb_promocode_categories.name
          end as Channel,
        plb_labs.name AS Lab_Name,
        plb_cities.name AS city_name,
        group_concat(distinct case 
          when plb_lab_tests.title is null then plb_tests_and_packages_masters.name
          else plb_lab_tests.title
          end SEPARATOR ', ') as tests,
          group_concat(distinct case 
          when plb_lab_tests.title is null then plb_tests_and_packages_masters.type
          else plb_lab_tests.type
          end SEPARATOR ', ') as tests_type,
        concat(plb_phlebos.first_name,' ',plb_phlebos.last_name) as phlebo_name,
          plb_customer_addresses.pincode,
        plb_bookings.total_actual_amount,
        plb_bookings.discount_amount,
        (plb_bookings.total_actual_amount-plb_bookings.discount_amount) as lab_mrp,
          plb_bookings.promocode_discount_amount,
          plb_bookings.redeem_coin,
          plb_bookings.total_paid_amount                 
          FROM plb_bookings
        LEFT Join plb_booking_tests on plb_bookings.id = plb_booking_tests.booking_id
        LEFT JOIN plb_promocodes ON plb_bookings.promocode = plb_promocodes.promocode
        LEFT JOIN plb_promocode_categories ON plb_promocodes.promocode_category = plb_promocode_categories.id
        LEFT JOIN plb_phlebos ON plb_phlebos.id = plb_bookings.phlebo_id
        LEFT JOIN plb_cities ON plb_cities.id = plb_bookings.city_id
        LEFT JOIN plb_customers AS c ON plb_bookings.customer_id = c.id 
        left join plb_customer_family_members on plb_bookings.family_member_id = plb_customer_family_members.id
        LEFT JOIN plb_customer_addresses ON plb_customer_addresses.id = plb_bookings.address_id
        LEFT JOIN plb_labs ON plb_labs.id = plb_bookings.lab_id
        LEFT JOIN plb_labs_branches ON plb_bookings.lab_branch_id = plb_labs_branches.id
        LEFT JOIN plb_lab_tests ON plb_lab_tests.id = plb_booking_tests.test_id
        left join plb_manages on plb_manages.id = plb_bookings.booking_by_id
        LEFT JOIN plb_tests_and_packages_masters on plb_lab_tests.test_and_package_id = plb_tests_and_packages_masters.id
        where  plb_bookings.booking_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 1 DAY)
        group by plb_bookings.id
        order by booking_date
        """)

        rows = cursor.fetchall()

        # Prepare header + data (same as before)
        data = [["Booking Id", "Booking Date","Booking Time", "Created Date", "Customer Name", "Mobile Number", "Gender",
                 "Date of Birth","Booking By","Created by Name","Customer Type","Booking Stage","Status","Booking Status",
                 "Promocode","Channel","Lab","City","Tests","Tests Type","Phlebo Name","Pincode","Actual Amount",
                 "Lab Discount","Lab MRP","Promocode Discount","Coins Discount","Revenue"]]

        for row in rows:
            formatted_row = []
            for val in row:

                if isinstance(val, datetime):
                    formatted_row.append(val.strftime("%Y-%m-%d %H:%M:%S"))

                elif isinstance(val, date):
                    formatted_row.append(val.strftime("%Y-%m-%d"))

                elif isinstance(val, timedelta):
                    # FIX: convert timedelta → string (e.g., "02:30:00")
                    formatted_row.append(str(val))

                elif val is None:
                    formatted_row.append("")

                else:
                    formatted_row.append(val)

            data.append(formatted_row)

        # ---- Write to Google Sheet ----
        sheet.clear()
        sheet.append_rows(data)

        cursor.close()
        db.close()

        logger.info(f"✅ Sheet updated — {len(rows)} rows")
        return "OK", 200

    except Exception as e:
        logger.exception("Error while updating sheet: %s", str(e)[:500])
        return "ERROR", 200


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)