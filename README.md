# 📈 Gold Data Warehouse Pipeline (XAU/USD)
**Project for Data Architecture (DE241)**

ระบบ Data Pipeline แบบ End-to-End สำหรับประมวลผลข้อมูลประวัติราคาทองคำสากล โดยเน้นการทำความสะอาดข้อมูล (Data Cleansing) และจัดการคุณภาพข้อมูล (Data Quality) ก่อนนำไปจัดเก็บในรูปแบบ Data Warehouse เพื่อรองรับการวิเคราะห์แนวโน้มการลงทุน

---

## 1. บทนำและวัตถุประสงค์ (Introduction & Objectives)
ตลาดทองคำมีความผันผวนสูงและข้อมูลมักมีข้อผิดพลาดแฝงอยู่ โปรเจกต์นี้จึงมุ่งเน้นการสร้างสถาปัตยกรรมข้อมูลที่แข็งแกร่ง โดยมีวัตถุประสงค์ดังนี้:
* **พัฒนาระบบ ETL (Extract, Transform, Load):** จัดการท่อส่งข้อมูลตั้งแต่ต้นทางจนถึงปลายทาง
* **ประยุกต์ใช้ Medallion Architecture:** วางโครงสร้างจัดเก็บแบบ Bronze, Silver และ Gold Layer
* **จัดการ Data Quality:** สร้างกลไกจัดการข้อมูลที่ขาดหาย (Missing Values) แบบอัตโนมัติ
* **ออกแบบ Data Model:** สร้างคลังข้อมูลแบบ Star Schema เพื่อให้พร้อมต่อการทำ BI (Business Intelligence)

## 2. ขอบเขตและลักษณะของข้อมูล (Data Scope & Characteristics)
* **Dataset:** ข้อมูลประวัติราคาทองคำเทียบดอลลาร์สหรัฐ (XAU/USD) จากแพลตฟอร์มการเงิน (อ้างอิงรูปแบบจาก Investing.com)
* **Timeframe:** ข้อมูลรายวัน ตั้งแต่ 1 มกราคม 2025 – ปัจจุบัน
* **Ingestion Method:** เป็นการประมวลผลแบบ **Batch Processing** ผ่านการจำลองข้อมูลวางใน Landing Zone ซึ่งช่วยลดข้อจำกัดเรื่อง Network/API Limit ทำให้การรัน Pipeline มีความเสถียร 100%

---

## 3. สถาปัตยกรรมและเครื่องมือ (Architecture & Tools)
ระบบใช้โครงสร้างการไหลของข้อมูลตามมาตรฐาน **Medallion Architecture** ควบคู่ไปกับเครื่องมือ Data Engineering ยุคใหม่

**เครื่องมือที่ใช้ (Tech Stack):**
* **Apache Airflow (บน Docker):** เป็น Orchestrator ควบคุมและจัดคิวการทำงาน
* **Python & Pandas:** เป็น Engine หลักในกระบวนการ Transform และ Cleansing
* **SQLite:** จำลองการทำงานของ Data Warehouse ปลายทาง

**Data Flow Pipeline:**
> `Landing Zone (Raw CSV)` ➔ `Bronze Layer (Backup)` ➔ `Silver Layer (Cleaned & DQ Checked)` ➔ `Gold Layer (Star Schema in SQLite)`
<img width="1205" height="192" alt="Screenshot 2026-05-16 091628" src="https://github.com/user-attachments/assets/40faaae1-68e5-4dba-af30-9b0d88bc5e8c" />

**Data Architecture Pipeline**

<img width="3477" height="7407" alt="Gold Data Transformation-2026-05-16-030435" src="https://github.com/user-attachments/assets/05bdcd43-73d7-49cd-8285-2f5508946415" />

## 4. โครงสร้างของโปรเจกต์ (Project Directory)
```text
gold-price-pipeline/
│
├── dags/
│   └── gold_ingestion_dag.py       # Source Code หลักของ Airflow DAG
├── data_lake/
│   ├── landing/
│   │   └── raw_gold_dataset.csv    # ข้อมูลดิบต้นฉบับก่อนเข้าสู่ระบบ
│   ├── bronze/                     # ข้อมูลดิบที่ถูกประทับเวลา (History)
│   ├── silver/                     # ข้อมูลที่ผ่านการ Transform แล้ว (CSV)
│   └── gold/
│       └── gold_data_warehouse.db  # ฐานข้อมูล SQLite ปลายทาง
├── docker-compose.yml              # Configuration สำหรับรัน Airflow
└── README.md                       # เอกสารโปรเจกต์
```
## 5. กระบวนการแปลงสภาพและคุณภาพข้อมูล (Data Transformation & DQ)
ข้อมูลดิบสายการเงินมักมีรูปแบบที่คอมพิวเตอร์นำไปคำนวณต่อไม่ได้ ระบบจึงต้องมีกระบวนการ Transform อย่างรัดกุม

* **ข้อมูลก่อนการแปลง (Raw Data - Landing/Bronze):** ข้อมูลมีชื่อคอลัมน์ไม่เป็นมาตรฐานสากล ตัวเลขมีเครื่องหมาย Comma (,) ทำให้ระบบมองเป็นข้อความ (String) และบางวันอาจมีข้อมูลสูญหาย (Null)
* **ขั้นตอนการทำความสะอาด (Transformation Process):**
  * **Standardize Columns:** เปลี่ยนชื่อคอลัมน์ให้เป็นมาตรฐาน เช่น `Date` เป็น `timestamp`, `Price` เป็น `price`
  * **Type Casting:** ลบเครื่องหมาย `,` ออก และแปลงชนิดข้อมูลจากข้อความให้เป็นตัวเลขทศนิยม (Float)
  * **Data Enrichment:** เพิ่มคอลัมน์ `currency` (USD) และ `source_file` เพื่อให้สามารถทำ Lineage Tracking ได้
  * **Data Quality Check (DQ):** ตรวจจับค่า `Null` ในคอลัมน์ราคา และใช้เทคนิค Forward Fill ในการดึงข้อมูลจากวันก่อนหน้ามาอุดช่องโหว่อัตโนมัติ
* **ข้อมูลหลังการแปลง (Processed Data - Silver):** ข้อมูลพร้อมใช้งานในรูปแบบตารางที่สะอาด มีคอลัมน์ครบถ้วน (timestamp, price, high_price, low_price, currency, source_file) และไร้ปัญหาช่องว่างของข้อมูล

## 6. การออกแบบฐานข้อมูล (Data Modeling)
ชั้น Gold Layer ถูกออกแบบด้วยแนวคิด Star Schema เพื่อเพิ่มประสิทธิภาพการ Query และลดความซ้ำซ้อนของข้อมูล

* **Fact Table (`fact_gold_prices`):** ตารางหลักที่ใช้เก็บข้อมูลเชิงตัวเลข (Metrics) ได้แก่ `price`, `high_price`, `low_price` และ `currency`
* **Dimension Table (`dim_date`):** ตารางมิติเวลาที่แตกข้อมูลวันที่ออกเป็น `date_key`, `day`, `month` และ `year` สำหรับใช้จัดกลุ่ม (Grouping) หรือกรองข้อมูล (Filtering)

<img width="2275" height="2970" alt="Gold Data Transformation-2026-05-16-042514" src="https://github.com/user-attachments/assets/a1419c09-dc76-48a7-9a3c-8855cc962df0" />

## 7. ตัวอย่างการนำข้อมูลไปวิเคราะห์ (Data Analytics & Insights)
ตัวอย่างคำสั่ง SQL ที่ใช้ดึง Insight จาก Data Warehouse สู่การวิเคราะห์เชิงลึก:

* **Query 1: ภาพรวมการเติบโตและความผันผวนรายเดือน (Monthly Performance & Volatility)**
```sql
WITH MonthlyStats AS (
    SELECT 
        d.year, 
        d.month, 
        ROUND(AVG(f.price), 2) AS avg_price,
        ROUND(MAX(f.high) - MIN(f.low), 2) AS volatility
    FROM fact_gold_prices f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT 
    year, 
    month, 
    avg_price,
    volatility,
    ROUND(((avg_price - LAG(avg_price) OVER (ORDER BY year, month)) / 
           LAG(avg_price) OVER (ORDER BY year, month)) * 100, 2) AS mom_growth_percent
FROM MonthlyStats
ORDER BY year DESC, month DESC;
```
<img width="852" height="505" alt="Screenshot 2026-05-16 111919" src="https://github.com/user-attachments/assets/51ce98cc-793f-47cb-8817-b42c185040d5" />

* **Query 2: ตรวจจับวันที่ราคาสวิงตัวรุนแรงที่สุด (Top 5 Max Daily Price Swings)**
```SQL
SELECT 
    d.full_date,
    f.high AS highest_price,
    f.low AS lowest_price,
    ROUND(f.high - f.low, 2) AS daily_price_swing
FROM fact_gold_prices f
JOIN dim_date d ON f.date_key = d.date_key
ORDER BY daily_price_swing DESC
LIMIT 5;
```
<img width="724" height="221" alt="Screenshot 2026-05-16 111927" src="https://github.com/user-attachments/assets/b30fad98-3166-4e18-9706-cc8a65d23b0c" />

บทสรุปและ Insight เชิงธุรกิจ:

ภาพรวมรายเดือน: สามารถติดตามเทรนด์ราคาขาขึ้นหรือขาลงได้จากการดึงค่าราคาเฉลี่ยในแต่ละเดือนมาเปรียบเทียบกัน

การเฝ้าระวังความเสี่ยง: ค่าความผันผวน (Volatility) ช่วยสะท้อนสภาวะตลาด หากความห่างระหว่างราคาสูงสุดและต่ำสุดในเดือนนั้นมีค่ามาก แสดงถึงสภาวะที่นักลงทุนมีความตื่นตระหนกสูง

โครงสร้างที่เอื้อต่อการวิเคราะห์: การมีตารางมิติเวลา (dim_date) แยกออกมา ทำให้ฝ่าย Data Analyst สามารถวิเคราะห์ข้อมูลเปรียบเทียบข้ามปี (YoY) หรือข้ามเดือน (MoM) ได้ทันที

การแยกมิติข้อมูล (Grain Separation): การแยกตาราง Daily Swing (Query 2) ออกมา ทำให้เห็นเหตุการณ์ระดับ Micro ได้ชัดเจนขึ้น ซึ่งเป็นประโยชน์ต่อนักลงทุนระยะสั้น (Day Trader) ในการประเมินความเสี่ยงรายวัน

## 8. วิธีการใช้งานและการติดตั้ง (Installation & Execution)
* **เตรียมข้อมูล:** นำไฟล์ Dataset ต้นฉบับไปวางไว้ในโฟลเดอร์ `data_lake/landing/raw_gold_dataset.csv`
* **เริ่มระบบ:** เปิด Terminal และรันคำสั่ง `docker-compose up -d` เพื่อเริ่มการทำงานของ Container Apache Airflow
* **สั่งรัน Pipeline:** เข้าใช้งาน Airflow Web UI ที่ `http://localhost:8080` และกด Trigger DAG ที่ชื่อ `gold_dataset_to_sqlite_pipeline`
* **ตรวจสอบผลลัพธ์:** นำไฟล์คลังข้อมูล `/data_lake/gold/gold_data_warehouse.db` ไปเปิดด้วยโปรแกรมจัดการฐานข้อมูล (เช่น SQLite Viewer หรือ DBeaver) เพื่อตรวจสอบความถูกต้อง

## 9. แนวทางการพัฒนาในอนาคต (Future Work)
* **Automated API Ingestion:** ยกระดับการดึงข้อมูลเป็นการเชื่อมต่อผ่าน API อัตโนมัติแบบรายวัน (Daily Fetch) เมื่อระบบโครงสร้างพื้นฐานมีความพร้อม
* **BI Integration:** เชื่อมต่อ Data Warehouse เข้ากับเครื่องมือ Business Intelligence (เช่น Power BI หรือ Tableau) เพื่อสร้าง Dashboard ติดตามราคาและเทรนด์การลงทุน
* **Cloud Data Warehouse:** ขยายสถาปัตยกรรมไปสู่ระบบ Cloud (เช่น Google BigQuery หรือ PostgreSQL) เพื่อรองรับปริมาณข้อมูลมหาศาลในระยะยาว
