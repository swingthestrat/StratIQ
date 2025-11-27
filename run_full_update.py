from ingest import run_ingestion
from populate_alerts import main as run_alerts
from database import Session, Alert, init_db
from datetime import datetime

def full_update():
    print("Starting Full System Update...")
    start_time = datetime.now()
    
    # 1. Ingest Data (Updates Universe + Fetches OHLCV + Aggregates)
    print("\n=== STEP 1: Data Ingestion ===")
    run_ingestion()
    
    # 2. Clear Alerts for Today (to ensure clean slate)
    print("\n=== STEP 2: Clearing Old Alerts ===")
    session = Session()
    try:
        today = datetime.now().date()
        deleted = session.query(Alert).filter(Alert.date == today).delete()
        session.commit()
        print(f"Cleared {deleted} alerts for today.")
    except Exception as e:
        print(f"Error clearing alerts: {e}")
        session.rollback()
    finally:
        session.close()
        
    # 3. Generate Alerts
    print("\n=== STEP 3: Alert Generation ===")
    run_alerts()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\nFull Update Complete in {duration}!")

if __name__ == "__main__":
    full_update()
