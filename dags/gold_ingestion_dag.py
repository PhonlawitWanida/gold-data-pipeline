from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import shutil
import sqlite3
import logging

logger = logging.getLogger(__name__)

# ==========================================
# 1. BRONZE: โหลดข้อมูลจาก Landing Zone
# ==========================================
def ingest_local_dataset():
    landing_path = "/opt/airflow/data_lake/landing/raw_gold_dataset.csv"
    bronze_dir = "/opt/airflow/data_lake/bronze/"
    os.makedirs(bronze_dir, exist_ok=True)
    
    if not os.path.exists(landing_path):
        raise FileNotFoundError(f"หาไฟล์ไม่เจอ! กรุณาเอาไฟล์ไปวางไว้ที่: {landing_path} และตั้งชื่อให้ตรงกัน")
    
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_path = f"{bronze_dir}gold_raw_{current_time}.csv"
    
    shutil.copy(landing_path, bronze_path)
    logger.info(f"Bronze Layer: โหลดไฟล์ Dataset ต้นฉบับสำเร็จ -> {bronze_path}")

# ==========================================
# 2. SILVER: ทำความสะอาดข้อมูล (Data Cleansing)
# ==========================================
def process_dataset_to_silver():
    bronze_dir = "/opt/airflow/data_lake/bronze/"
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    
    all_files = [os.path.join(bronze_dir, f) for f in os.listdir(bronze_dir) if f.endswith('.csv')]
    latest_file = max(all_files, key=os.path.getctime)
    
    df = pd.read_csv(latest_file)
    
    # เปลี่ยนชื่อคอลัมน์ตามไฟล์ของ Investing.com
    df = df.rename(columns={'Date': 'timestamp', 'Price': 'price', 'High': 'high_price', 'Low': 'low_price'})
    
    # ล้างเครื่องหมายลูกน้ำ (Comma) ออก และแปลงเป็นตัวเลข
    for col in ['price', 'high_price', 'low_price']:
        if df[col].dtype == 'object':
            df[col] = df[col].str.replace(',', '').astype(float)
            
    df['currency'] = 'USD'
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp']) 
    df['source_file'] = os.path.basename(latest_file)
    
    expected_columns = ['timestamp', 'price', 'high_price', 'low_price', 'currency', 'source_file']
    df = df.reindex(columns=expected_columns)
    
    df.to_csv(silver_path, index=False)
    logger.info(f"Silver Layer: ทำความสะอาดและแปลงข้อมูลสำเร็จ {len(df)} แถว")

# ==========================================
# 3. DATA QUALITY
# ==========================================
def check_data_quality():
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    df = pd.read_csv(silver_path)
    
    if df['price'].isnull().any():
        logger.warning("DQ Warning: พบราคาเป็นค่าว่าง จัดการเติมข้อมูลให้ (Forward Fill)")
        df['price'] = df['price'].ffill()
        df.to_csv(silver_path, index=False)
        
    logger.info("✅ DQ Passed: ข้อมูลพร้อมใช้งาน")

# ==========================================
# 4. GOLD: สร้าง Star Schema และนำเข้า SQLite 🌟
# ==========================================
def transform_to_star_schema_sqlite():
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    gold_dir = "/opt/airflow/data_lake/gold/"
    os.makedirs(gold_dir, exist_ok=True)
    
    df = pd.read_csv(silver_path)
    df['ts_obj'] = pd.to_datetime(df['timestamp'])
    
    # สร้าง Dimension Table
    dim_date = pd.DataFrame({
        'date_key': df['ts_obj'].dt.strftime('%Y%m%d'),
        'full_date': df['ts_obj'].dt.date.astype(str), # แปลงเป็น string เพื่อให้ SQLite อ่านง่าย
        'day': df['ts_obj'].dt.day,
        'month': df['ts_obj'].dt.month,
        'year': df['ts_obj'].dt.year
    }).drop_duplicates()
    
    # สร้าง Fact Table
    fact_gold = pd.DataFrame({
        'date_key': df['ts_obj'].dt.strftime('%Y%m%d'),
        'price': df['price'],
        'high': df['high_price'],
        'low': df['low_price'],
        'currency': df['currency']
    })
    
    # 🌟 เชื่อมต่อฐานข้อมูล SQLite (ระบบจะสร้างไฟล์ .db ให้ถ้ายังไม่มี)
    db_path = f"{gold_dir}gold_data_warehouse.db"
    conn = sqlite3.connect(db_path)
    
    # บันทึกตารางลง SQLite (ใช้ if_exists='replace' เพื่อทับของเก่าถ้ารันซ้ำ)
    dim_date.to_sql('dim_date', conn, if_exists='replace', index=False)
    fact_gold.to_sql('fact_gold_prices', conn, if_exists='replace', index=False)
    
    # ปิดการเชื่อมต่อ
    conn.close()
    
    logger.info(f"Gold Layer: นำข้อมูล Star Schema เข้าสู่ฐานข้อมูล SQLite เรียบร้อยแล้ว! (ไฟล์อยู่ที่: {db_path})")

# ==========================================
# DAG CONFIG (สไตล์ E-T-L)
# ==========================================
with DAG(
    'gold_dataset_to_sqlite_pipeline',
    default_args={'owner': 'Phonlawit', 'start_date': datetime(2026, 5, 15)},
    schedule_interval='@once', 
    catchup=False,
    tags=['dataset', 'sqlite', 'etl']
) as dag:

    # E = Extract (ดึงข้อมูลเข้า Bronze)
    t1_extract = PythonOperator(
        task_id='extract_to_bronze', 
        python_callable=ingest_local_dataset
    )
    
    # T = Transform (ทำความสะอาดใน Silver)
    t2_transform_clean = PythonOperator(
        task_id='transform_clean_to_silver', 
        python_callable=process_dataset_to_silver
    )
    
    # T = Transform (ตรวจสอบ Data Quality)
    t3_transform_dq = PythonOperator(
        task_id='transform_data_quality_check', 
        python_callable=check_data_quality
    )
    
    # L = Load (สร้าง Star Schema และโหลดเข้า SQLite)
    t4_load = PythonOperator(
        task_id='load_to_sqlite_warehouse', 
        python_callable=transform_to_star_schema_sqlite 
    )

    # วางท่อ E -> T -> L
    t1_extract >> t2_transform_clean >> t3_transform_dq >> t4_load