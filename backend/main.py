from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from database import get_db_connection

app = FastAPI(title="Data Visualization API")

# Configure CORS to allow requests from the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Data Visualization API"}

@app.get("/health")
def health_check():
    conn = get_db_connection()
    if conn:
        conn.close()
        return {"status": "ok", "database": "connected"}
    else:
        raise HTTPException(status_code=500, detail="Database connection failed")

@app.get("/api/full-data")
def get_full_data():
    conn = get_db_connection()
    if not conn:
         raise HTTPException(status_code=500, detail="Database connection failed")
    
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    c.country_iso3_id,
                    c.country_name,
                    p.year_id as year,
                    p.population,
                    cr.convicts_per_100000,
                    i.immigration_per_100000
                FROM country c
                JOIN population p ON c.country_iso3_id = p.country_iso3_id
                LEFT JOIN crime cr ON c.country_iso3_id = cr.country_iso3_id AND p.year_id = cr.year_id
                LEFT JOIN immigration i ON c.country_iso3_id = i.country_iso3_id AND p.year_id = i.year_id
                ORDER BY c.country_name, p.year_id;
            """
            cur.execute(query)
            data = cur.fetchall()
            return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
