from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import glob
import pandas as pd
import logging

# ตั้งค่า Logging (System Observability)
logger = logging.getLogger(__name__)

# ==========================================
# 1. Bronze & Silver Layer (คงเดิมแต่เพิ่ม Metadata)
# ==========================================
def fetch_gold_data():
    url = "https://www.goldapi.io/api/XAU/USD"
    headers = {"x-access-token": "goldapi-4fa91653d81f7bde30022e6f5ac87479-io", "Content-Type": "application/json"}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = f"/opt/airflow/data_lake/bronze/gold_raw_{current_time}.json"
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'w') as f:
            json.dump(data, f)
        logger.info(f"Bronze Layer: Success. Ingested 1 record to {save_path}")
    else:
        logger.error(f"Bronze Layer: Failed. Status Code: {response.status_code}")
        raise Exception("API Request Failed")

def transform_bronze_to_silver():
    bronze_dir = "/opt/airflow/data_lake/bronze/"
    list_of_files = glob.glob(os.path.join(bronze_dir, '*.json'))
    latest_file = max(list_of_files, key=os.path.getctime)
    
    with open(latest_file, 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame([data])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s') + pd.Timedelta(hours=7)
    
    # เพิ่ม Lineage Metadata (บอกแหล่งที่มาของข้อมูล)
    df['source_file'] = os.path.basename(latest_file)
    df['processed_at'] = datetime.now()
    
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    file_exists = os.path.isfile(silver_path)
    df.to_csv(silver_path, mode='a', index=False, header=not file_exists)
    logger.info(f"Silver Layer: Success. Processed file {latest_file}")

# ==========================================
# 2. Data Quality & Observability
# ==========================================
def check_data_quality():
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    df = pd.read_csv(silver_path)
    
    # DQ Logic
    errors = []
    if df['price'].isnull().any(): errors.append("Null price detected")
    if (df['price'] <= 0).any(): errors.append("Negative/Zero price detected")
    
    if errors:
        error_msg = f"🚨 Data Quality Alert: {', '.join(errors)}"
        logger.error(error_msg)
        # จำลองการส่ง Alert (เช่น Slack/LINE)
        print(f"SENDING ALERT TO ADMIN: {error_msg}")
        raise ValueError(error_msg)
    
    logger.info("✅ Data Quality Check: All dimensions passed.")

# ==========================================
# 3. Gold Layer: Star Schema Transformation 🌟
# ==========================================
def transform_silver_to_gold_star():
    silver_path = "/opt/airflow/data_lake/silver/gold_prices_silver.csv"
    df = pd.read_csv(silver_path)
    
    # สร้าง dim_date
    df['dt_obj'] = pd.to_datetime(df['timestamp'])
    dim_date = pd.DataFrame({
        'date_key': df['dt_obj'].dt.strftime('%Y%m%d'),
        'full_date': df['dt_obj'].dt.date,
        'day': df['dt_obj'].dt.day,
        'month': df['dt_obj'].dt.month,
        'year': df['dt_obj'].dt.year,
        'day_name': df['dt_obj'].dt.day_name()
    }).drop_duplicates()
    
    # สร้าง fact_gold_prices
    fact_gold = pd.DataFrame({
        'fact_key': range(len(df)),
        'date_key': df['dt_obj'].dt.strftime('%Y%m%d'),
        'price': df['price'],
        'high': df['high_price'],
        'low': df['low_price'],
        'currency': df['currency'],
        'timestamp': df['timestamp']
    })
    
    # บันทึกไฟล์แยกเป็นตาราง (Star Schema)
    gold_dir = "/opt/airflow/data_lake/gold/"
    os.makedirs(gold_dir, exist_ok=True)
    dim_date.to_csv(f"{gold_dir}dim_date.csv", index=False)
    fact_gold.to_csv(f"{gold_dir}fact_gold_prices.csv", index=False)
    
    logger.info(f"Gold Layer Star Schema: Success. Created {len(dim_date)} dates and {len(fact_gold)} facts.")

# ==========================================
# DAG Definition
# ==========================================
with DAG(
    'gold_price_pipeline',
    default_args={'owner': 'Phonlawit', 'start_date': datetime(2026, 5, 14)},
    schedule_interval='@hourly',
    catchup=False,
    tags=['star_schema', 'observability']
) as dag:

    task_bronze = PythonOperator(task_id='extract_bronze', python_callable=fetch_gold_data)
    task_silver = PythonOperator(task_id='transform_silver', python_callable=transform_bronze_to_silver)
    task_dq = PythonOperator(task_id='data_quality_check', python_callable=check_data_quality)
    task_gold = PythonOperator(task_id='transform_gold_star', python_callable=transform_silver_to_gold_star)

    task_bronze >> task_silver >> task_dq >> task_gold