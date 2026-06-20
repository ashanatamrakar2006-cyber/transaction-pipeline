# Transaction Processing Pipeline

AI-Powered transaction processing pipeline built with FastAPI, PostgreSQL, Celery, Redis, and Gemini LLM.

## Tech Stack
- **API**: FastAPI
- **Database**: PostgreSQL
- **Job Queue**: Celery + Redis
- **LLM**: Google Gemini 1.5 Flash
- **Containerisation**: Docker + Docker Compose

## Setup Instructions

### Prerequisites
- Docker Desktop installed
- Gemini API Key (free from aistudio.google.com)

### Steps

1. Clone the repository:
git clone https://github.com/ashanatamrakar2006-cyber/transaction-pipeline

cd transaction-pipeline

2. Add your Gemini API key in `docker-compose.yml`:
GEMINI_API_KEY=your_key_here

3. Start everything with one command:
docker compose up --build

4. API will be running at:
http://localhost:8000

http://localhost:8000/docs

## API Endpoints

### 1. Upload CSV File
POST /jobs/upload
```bash
curl -X POST "http://localhost:8000/jobs/upload" \
  -H "accept: application/json" \
  -F "file=@transactions.csv"
```

### 2. Check Job Status
GET /jobs/{job_id}/status
```bash
curl -X GET "http://localhost:8000/jobs/{job_id}/status"
```

### 3. Get Full Results
GET /jobs/{job_id}/results
```bash
curl -X GET "http://localhost:8000/jobs/{job_id}/results"
```

### 4. List All Jobs
GET /jobs
```bash
curl -X GET "http://localhost:8000/jobs"
curl -X GET "http://localhost:8000/jobs?status=completed"
```

## Processing Pipeline
1. **Data Cleaning** - Normalize dates, strip currency symbols, uppercase status
2. **Anomaly Detection** - Flag transactions > 3x account median
3. **LLM Classification** - Gemini classifies uncategorized transactions
4. **LLM Summary** - Gemini generates spending narrative and risk level