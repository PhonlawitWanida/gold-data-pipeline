from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import glob
import pandas as pd

# ==========================================
# 1. ฟังก์ชันดึงข้อมูล (Bronze Layer)
# ==========================================
def fetch_gold_data():
    url = "https://www.goldapi.io/api/XAU/USD"
    headers = {
        "x-access-token": "goldapi-4fa91653d81f7bde30022e6f5ac87479-io",
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = f"/opt/airflow/data_lake/bronze/gold_raw_{current_time}.json"
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        with open(save_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Bronze Layer: บันทึกไฟล์สำเร็จที่ {save_path}")
    else:
        raise Exception("API Request Failed")

# ==========================================
# 2. ฟังก์ชันทำความสะอาดข้อมูล (Silver Layer)
# ==========================================
def transform_bronze_to_silver():
    bronze_dir = "/opt/airflow/data_lake/bronze/"
    list_of_files = glob.glob(os.path.join(bronze_dir, '*.json'))
    
    if not list_of_files:
        raise Exception("Silver Layer Error: ไม่พบไฟล์ข้อมูลในชั้น Bronze")
        
    latest_file = max(list_of_files, key=os.path.getctime)
    print(f"Silver Layer: กำลังประมวลผลไฟล์ -> {latest_file}")
    
    with open(latest_file, 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame([data])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s') + pd.Timedelta(hours=7)
    
    columns_to_keep = ['timestamp', 'metal', 'currency', 'price', 'open_price', 'high_price', 'low_price']
    df_clean = df[columns_to_keep]
    
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    
    file_exists = os.path.isfile(silver_path)
    df_clean.to_csv(silver_path, mode='a', index=False, header=not file_exists)
    print(f"Silver Layer: ทำความสะอาดและบันทึกข้อมูลสำเร็จที่ {silver_path}")

# ==========================================
# 3. ฟังก์ชันตรวจสอบคุณภาพข้อมูล (Data Quality Check) - 🌟 เพิ่มใหม่
# ==========================================
def check_data_quality():
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    
    if not os.path.isfile(silver_path):
        raise Exception("Data Quality Error: ไม่พบไฟล์ Silver Layer ให้ตรวจสอบ")
    
    # อ่านข้อมูลชั้น Silver ขึ้นมาตรวจสอบ
    df = pd.read_csv(silver_path)
    
    # Check 1: Completeness (ความครบถ้วน) - ต้องไม่มีค่าว่างในคอลัมน์ price
    if df['price'].isnull().any():
        raise ValueError("🚨 Data Quality Alert: พบค่าว่าง (Null) ในคอลัมน์ราคาทองคำ!")
        
    # Check 2: Validity (ความสมเหตุสมผล) - ราคาทองต้องมากกว่า 0 เสมอ
    if (df['price'] <= 0).any():
        raise ValueError("🚨 Data Quality Alert: พบราคาทองคำติดลบหรือเท่ากับศูนย์ ซึ่งเป็นไปไม่ได้!")
        
    # Check 3: Validity - ต้องเป็นข้อมูลของทองคำ (XAU) และสกุลเงินดอลลาร์ (USD) เท่านั้น
    if not (df['metal'] == 'XAU').all() or not (df['currency'] == 'USD').all():
        raise ValueError("🚨 Data Quality Alert: พบข้อมูลที่ไม่ได้เป็นสกุลเงิน XAU/USD หลุดเข้ามา!")

    print("✅ Data Quality Check Passed: ข้อมูลถูกต้อง ครบถ้วน พร้อมนำไปใช้งานต่อ")

# ==========================================
# 4. ฟังก์ชันสรุปผลข้อมูลธุรกิจ (Gold Layer) - 🌟 เพิ่มใหม่
# ==========================================
def transform_silver_to_gold():
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    
    if not os.path.isfile(silver_path):
        raise Exception("Gold Layer Error: ไม่พบไฟล์ Silver Layer")
    
    # 1. อ่านข้อมูลที่ผ่านการ Clean และ DQ Check มาแล้ว
    df = pd.read_csv(silver_path)
    
    # 2. สร้างคอลัมน์ 'date' โดยตัดเวลาทิ้ง เพื่อใช้จัดกลุ่ม (Group By) เป็นรายวัน
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    
    # 3. คำนวณสถิติทางธุรกิจ (Business Metrics) สำหรับการเทรดวิเคราะห์
    gold_summary = df.groupby('date').agg(
        avg_price=('price', 'mean'),             # ราคาเฉลี่ยของวัน
        max_price=('price', 'max'),              # ราคาสูงสุดของวัน (Resistance)
        min_price=('price', 'min'),              # ราคาต่ำสุดของวัน (Support)
        volatility_spread=('price', lambda x: x.max() - x.min()), # ความผันผวน (ส่วนต่างจุดสูงสุด-ต่ำสุด)
        data_points=('price', 'count')           # จำนวนครั้งที่ดึงข้อมูลในวันนั้น
    ).reset_index()
    
    # 4. สร้าง Moving Average (MA) 3 วัน เพื่อดูเทรนด์ระยะสั้น
    gold_summary['moving_avg_3d'] = gold_summary['avg_price'].rolling(window=3, min_periods=1).mean()
    
    # ปัดเศษทศนิยมให้ดูสวยงาม (2 ตำแหน่งตามมาตรฐานค่าเงิน)
    gold_summary = gold_summary.round(2)
    
    # 5. กำหนดที่เซฟไฟล์ใน Gold Layer
    gold_path = "/opt/airflow/data_lake/gold/gold_daily_summary.csv"
    os.makedirs(os.path.dirname(gold_path), exist_ok=True)
    
    # 💡 ทริค: ในชั้น Gold เรามักจะ "เขียนทับ (Overwrite)" ไฟล์เดิมเสมอ 
    # เพื่อให้ Dashboard ได้ตารางสรุปผลที่อัปเดตล่าสุดไปใช้แบบบรรทัดไม่ซ้ำซ้อน
    gold_summary.to_csv(gold_path, index=False, mode='w')
    
    print(f"Gold Layer: สร้างตารางสรุปผลรายวันสำเร็จที่ {gold_path}")
    print(gold_summary.tail(3)) # ปริ้นท์ 3 วันล่าสุดให้ดูใน Log

# ==========================================
# ตั้งค่า DAG และร้อยเรียง Task
# ==========================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'gold_price_pipeline',
    default_args=default_args,
    description='End-to-End Gold Price Pipeline (Medallion Architecture)',
    schedule_interval='@hourly',
    catchup=False,
    tags=['gold_pipeline'],
) as dag:

    task_bronze = PythonOperator(
        task_id='extract_gold_api_to_bronze',
        python_callable=fetch_gold_data
    )

    task_silver = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver
    )

    task_dq_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality
    )
    
    task_gold = PythonOperator(
        task_id='transform_silver_to_gold',
        python_callable=transform_silver_to_gold
    )

    # 🌟 ผูก Master Pipeline (Data Lineage) 🌟
    task_bronze >> task_silver >> task_dq_check >> task_gold
    
# ==========================================
# ตั้งค่า DAG และร้อยเรียง Task
# ==========================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'gold_price_pipeline',
    default_args=default_args,
    description='End-to-End Gold Price Pipeline (Bronze -> Silver -> DQ)',
    schedule_interval='@hourly',
    catchup=False,
    tags=['gold_pipeline'],
) as dag:

    task_bronze = PythonOperator(
        task_id='extract_gold_api_to_bronze',
        python_callable=fetch_gold_data
    )

    task_silver = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver
    )

    task_dq_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality
    )

    # กำหนดลำดับการทำงาน (Pipeline Flow)
    task_bronze >> task_silver >> task_dq_check