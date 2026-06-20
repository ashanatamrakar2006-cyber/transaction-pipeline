import os
import uuid
import shutil
from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from app.database import engine, get_db
from app.models import Job, Transaction, JobSummary
from app.tasks import process_csv_task
import app.models as models

load_dotenv()

# Create all database tables on startup
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Transaction Processing Pipeline")

# Folder to save uploaded CSV files
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.get("/")
def read_root():
    return {"message": "Transaction pipeline is running!"}

# ---- ENDPOINT 1: Upload CSV ----
@app.post("/jobs/upload")
def upload_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    # Validate file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files allowed!")

    # Save file to uploads folder
    job_id = str(uuid.uuid4())
    file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # Create job record in database
    job = Job(
        id=job_id,
        filename=file.filename,
        status="pending"
    )
    db.add(job)
    db.commit()

    # Send task to Celery worker
    process_csv_task.delay(job_id, file_path)

    return {
        "job_id": job_id,
        "filename": file.filename,
        "status": "pending",
        "message": "File uploaded successfully! Use job_id to check status."
    }

# ---- ENDPOINT 2: Check Job Status ----
@app.get("/jobs/{job_id}/status")
def get_job_status(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found!")

    response = {
        "job_id": job.id,
        "status": job.status,
        "filename": job.filename,
        "created_at": job.created_at
    }

    # If completed, include summary
    if job.status == "completed":
        summary = db.query(JobSummary).filter(JobSummary.job_id == job_id).first()
        if summary:
            response["summary"] = {
                "total_spend_inr": summary.total_spend_inr,
                "total_spend_usd": summary.total_spend_usd,
                "anomaly_count": summary.anomaly_count,
                "risk_level": summary.risk_level,
                "narrative": summary.narrative
            }

    return response

# ---- ENDPOINT 3: Get Full Results ----
@app.get("/jobs/{job_id}/results")
def get_job_results(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found!")

    if job.status != "completed":
        raise HTTPException(status_code=400, detail=f"Job is still {job.status}")

    # Get all transactions
    transactions = db.query(Transaction).filter(Transaction.job_id == job_id).all()

    # Get anomalies
    anomalies = [t for t in transactions if t.is_anomaly]

    # Category wise spend breakdown
    category_spend = {}
    for t in transactions:
        cat = t.category or "Uncategorised"
        category_spend[cat] = category_spend.get(cat, 0) + t.amount

    # Get summary
    summary = db.query(JobSummary).filter(JobSummary.job_id == job_id).first()

    return {
        "job_id": job_id,
        "total_transactions": len(transactions),
        "transactions": [
            {
                "txn_id": t.txn_id,
                "date": t.date,
                "merchant": t.merchant,
                "amount": t.amount,
                "currency": t.currency,
                "status": t.status,
                "category": t.category,
                "is_anomaly": t.is_anomaly,
                "anomaly_reason": t.anomaly_reason
            } for t in transactions
        ],
        "anomalies": [
            {
                "txn_id": t.txn_id,
                "merchant": t.merchant,
                "amount": t.amount,
                "reason": t.anomaly_reason
            } for t in anomalies
        ],
        "category_spend": category_spend,
        "summary": {
            "total_spend_inr": summary.total_spend_inr,
            "total_spend_usd": summary.total_spend_usd,
            "top_merchants": summary.top_merchants,
            "anomaly_count": summary.anomaly_count,
            "narrative": summary.narrative,
            "risk_level": summary.risk_level
        } if summary else {}
    }

# ---- ENDPOINT 4: List All Jobs ----
@app.get("/jobs")
def list_jobs(status: str = Query(None), db: Session = Depends(get_db)):
    query = db.query(Job)

    # Filter by status if provided
    if status:
        query = query.filter(Job.status == status)

    jobs = query.order_by(Job.created_at.desc()).all()

    return {
        "total": len(jobs),
        "jobs": [
            {
                "job_id": job.id,
                "filename": job.filename,
                "status": job.status,
                "row_count_raw": job.row_count_raw,
                "row_count_clean": job.row_count_clean,
                "created_at": job.created_at
            } for job in jobs
        ]
    }