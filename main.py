from fastapi import FastAPI, HTTPException
import uvicorn
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

# --- Project Structure Imports ---
from database import DatabaseManager # Use the shared, production-ready DB manager
import ollama
from scheduler import scheduler, schedule_jobs # Import our new scheduler

# --- Lifespan Management for Scheduler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the application's startup and shutdown events.
    This is the modern way to handle background tasks in FastAPI.
    """
    # This code runs on startup
    print("INFO:     Starting up the application...")
    schedule_jobs()  # Define and schedule our recurring jobs
    scheduler.start()  # Start the scheduler in the background
    yield
    # This code runs on shutdown
    print("INFO:     Shutting down the application...")
    scheduler.shutdown()

# Create an instance of the FastAPI class with the lifespan manager
app = FastAPI(
    title="FinsightAI API",
    description="API for providing AI-driven financial insights.",
    version="0.1.0",
    lifespan=lifespan
)

# --- API Endpoints ---

@app.get("/")
def read_root():
    """
    Root endpoint for the API. Returns a welcome message.
    """
    return {"message": "Welcome to the FinsightAI API! Background jobs are running."}


@app.get("/insight")
def get_stock_insight(ticker: str):
    """
    This is the core endpoint of our MVP. It takes a stock ticker,
    gathers the relevant data, uses an AI model to generate an insight,
    and returns it to the user.
    """
    if not ticker.endswith('.NS'):
        raise HTTPException(status_code=400, detail="Invalid ticker format. Ticker must end with '.NS'")

    db_manager = None
    try:
        # We instantiate DatabaseManager here as it's needed for this specific request
        db_manager = DatabaseManager()
        with db_manager.get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("SELECT id, long_name FROM securities WHERE ticker = %s;", (ticker,))
                security_record = cursor.fetchone()
                if not security_record:
                    raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found in the database.")
                security_id = security_record['id']
                stock_name = security_record['long_name']

                seven_days_ago = datetime.now().date() - timedelta(days=7)
                cursor.execute(
                    "SELECT trade_date, close_price FROM daily_prices WHERE security_id = %s AND trade_date >= %s ORDER BY trade_date DESC;",
                    (security_id, seven_days_ago)
                )
                price_records = cursor.fetchall()
                recent_prices = ", ".join([f"{rec['trade_date'].strftime('%b %d')}: ₹{rec['close_price']:.2f}" for rec in price_records])

                cursor.execute(
                    "SELECT title FROM news_articles WHERE security_id = %s ORDER BY published_at DESC LIMIT 5;",
                    (security_id,)
                )
                news_records = cursor.fetchall()
                recent_news = " ".join([f"{i+1}. {rec['title']}" for i, rec in enumerate(news_records)])

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"❌ DATABASE ERROR: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching data.")
    finally:
        if db_manager and db_manager.pool:
            # Close the pool when the app is done with it for this request
            # Note: In a real high-traffic app, you might manage the pool differently
            db_manager.pool.closeall()


    try:
        prompt = f"""
        You are an expert financial analyst for Indian retail investors. Your goal is to provide a concise, unbiased, and easy-to-understand insight based on the data provided. Do not give financial advice.

        Stock: {stock_name} ({ticker})
        Recent Price Action: {recent_prices}
        Recent News Headlines: {recent_news}

        Based on the data above, provide a 2-3 sentence summary of the current situation for this stock. Analyze the sentiment from the news and the trend from the price action.
        """
        response = ollama.chat(
            model='llama3:70b',
            messages=[{'role': 'user', 'content': prompt}]
        )
        ai_insight = response['message']['content']
    except Exception as e:
        print(f"❌ LLM ERROR: {e}")
        ai_insight = "The AI insight generator is currently unavailable."

    return {
        "ticker": ticker,
        "context": {"prices": recent_prices, "news": recent_news},
        "insight": ai_insight
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
