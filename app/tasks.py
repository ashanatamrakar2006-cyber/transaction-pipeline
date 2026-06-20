import os
import pandas as pd
from google import genai
from celery import Celery
from datetime import datetime
from dotenv import load_dotenv
from app.database import SessionLocal
from app.models import Transaction, Job, JobSummary
import logging
import json
import time

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup Celery with Redis
celery_app = Celery(
    "tasks",
    broker="redis://redis:6379/0",
    backend="redis://redis:6379/0"
)

def get_gemini_client():
    """Create Gemini client only when needed"""
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("GEMINI_API_KEY not set!")
    return genai.Client(api_key=api_key)

def call_gemini_with_retry(prompt, max_retries=3):
    """Call Gemini API with exponential backoff retry"""
    for attempt in range(max_retries):
        try:
            client = get_gemini_client()
            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=prompt
            )
            return response.text
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Gemini failed after {max_retries} attempts: {e}")
                return None
            wait_time = 2 ** attempt
            logger.warning(f"Gemini attempt {attempt + 1} failed, retrying in {wait_time}s")
            time.sleep(wait_time)

@celery_app.task(bind=True)
def process_csv_task(self, job_id: str, file_path: str):
    """Main Celery task to process CSV file"""
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        job.status = "processing"
        db.commit()

        logger.info(f"Processing job: {job_id}")

        df = pd.read_csv(file_path)
        job.row_count_raw = len(df)
        db.commit()

        # ---- STEP 1: Data Cleaning ----
        def parse_date(date_str):
            for fmt in ("%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d"):
                try:
                    return datetime.strptime(str(date_str), fmt).strftime("%Y-%m-%d")
                except:
                    continue
            return str(date_str)

        df['date'] = df['date'].apply(parse_date)
        df['amount'] = df['amount'].astype(str).str.replace(r'[\$,]', '', regex=True)
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
        df['status'] = df['status'].str.upper().fillna('UNKNOWN')
        df['currency'] = df['currency'].str.upper().fillna('INR')
        df['category'] = df['category'].fillna('')
        df = df.drop_duplicates()

        job.row_count_clean = len(df)
        db.commit()

        # ---- STEP 2: Anomaly Detection ----
        median = df.groupby('account_id')['amount'].transform('median')
        df['is_anomaly'] = df['amount'] > (3 * median)
        df['anomaly_reason'] = ''
        df.loc[df['is_anomaly'], 'anomaly_reason'] = 'Amount exceeds 3x account median'

        domestic_merchants = ['swiggy', 'ola', 'irctc', 'zomato', 'flipkart']
        domestic_mask = (
            df['currency'] == 'USD') & (
            df['merchant'].str.lower().str.contains('|'.join(domestic_merchants), na=False)
        )
        df.loc[domestic_mask, 'is_anomaly'] = True
        df.loc[domestic_mask, 'anomaly_reason'] = 'USD currency used with domestic merchant'

        # ---- STEP 3: LLM Category Classification ----
        uncategorized = df[df['category'] == '']
        df['llm_category'] = ''
        df['llm_failed'] = False

        if not uncategorized.empty:
            merchants_list = uncategorized['merchant'].tolist()
            merchants_str = "\n".join([f"{i+1}. {m}" for i, m in enumerate(merchants_list)])

            prompt = f"""
            Classify each merchant into one of these categories:
            Food, Shopping, Travel, Transport, Utilities, Cash Withdrawal, Entertainment, Other

            Merchants:
            {merchants_str}

            Return ONLY a JSON array like: ["Food", "Shopping", "Travel"]
            One category per merchant in the same order.
            """

            response = call_gemini_with_retry(prompt)

            if response:
                try:
                    clean = response.strip().replace('```json', '').replace('```', '').strip()
                    categories = json.loads(clean)
                    df.loc[df['category'] == '', 'llm_category'] = categories
                except:
                    df.loc[df['category'] == '', 'llm_failed'] = True
            else:
                df.loc[df['category'] == '', 'llm_failed'] = True

        # ---- STEP 4: LLM Narrative Summary ----
        total_inr = df[df['currency'] == 'INR']['amount'].sum()
        total_usd = df[df['currency'] == 'USD']['amount'].sum()
        top_merchants = df.groupby('merchant')['amount'].sum().nlargest(3).index.tolist()
        anomaly_count = df['is_anomaly'].sum()

        summary_prompt = f"""
        Generate a spending summary in JSON format.
        Total INR spend: {total_inr}
        Total USD spend: {total_usd}
        Top merchants: {top_merchants}
        Anomaly count: {anomaly_count}
        Total transactions: {len(df)}

        Return ONLY this JSON:
        {{
            "total_spend_inr": {total_inr},
            "total_spend_usd": {total_usd},
            "top_merchants": {json.dumps(top_merchants)},
            "anomaly_count": {int(anomaly_count)},
            "narrative": "2-3 sentence spending summary here",
            "risk_level": "low/medium/high"
        }}
        """

        summary_response = call_gemini_with_retry(summary_prompt)
        summary_data = {}

        if summary_response:
            try:
                clean = summary_response.strip().replace('```json', '').replace('```', '').strip()
                summary_data = json.loads(clean)
            except:
                summary_data = {
                    "total_spend_inr": total_inr,
                    "total_spend_usd": total_usd,
                    "top_merchants": top_merchants,
                    "anomaly_count": int(anomaly_count),
                    "narrative": "Summary generation failed.",
                    "risk_level": "medium"
                }

        # ---- STEP 5: Save to Database ----
        for _, row in df.iterrows():
            txn = Transaction(
                job_id=job_id,
                txn_id=str(row.get('txn_id', '')),
                date=row['date'],
                merchant=str(row.get('merchant', '')),
                amount=float(row['amount']),
                currency=row['currency'],
                status=row['status'],
                category=row['category'] if row['category'] else row.get('llm_category', 'Uncategorised'),
                account_id=str(row.get('account_id', '')),
                is_anomaly=bool(row['is_anomaly']),
                anomaly_reason=row.get('anomaly_reason', ''),
                llm_category=row.get('llm_category', ''),
                llm_failed=bool(row.get('llm_failed', False))
            )
            db.add(txn)

        job_summary = JobSummary(
    job_id=job_id,
    total_spend_inr=float(summary_data.get('total_spend_inr', total_inr)),
    total_spend_usd=float(summary_data.get('total_spend_usd', total_usd)),
    top_merchants=summary_data.get('top_merchants', top_merchants),
    anomaly_count=int(summary_data.get('anomaly_count', anomaly_count)),
    narrative=str(summary_data.get('narrative', 'No narrative available')),
    risk_level=str(summary_data.get('risk_level', 'medium'))
)
        db.add(job_summary)

        job.status = "completed"
        job.completed_at = datetime.utcnow()
        db.commit()

        logger.info(f"Job {job_id} completed successfully!")
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Job {job_id} failed: {str(e)}")
        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            job.status = "failed"
            job.error_message = str(e)
            db.commit()
        raise e
    finally:
        db.close()