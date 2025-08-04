from fastapi import FastAPI, HTTPException
import uvicorn
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime, timedelta

# --- Project Structure Imports ---
# This allows our API to access the logic from our other services.
from stock_data_service.database import get_db_connection
# from stock_data_service.news_service import generate_embedding # We might need this later
import ollama

# Create an instance of the FastAPI class
app = FastAPI(
    title="FinsightAI API",
    description="API for providing AI-driven financial insights.",
    version="0.1.0"
)

# --- API Endpoints ---

@app.get("/")
def read_root():
    """
    Root endpoint for the API. Returns a welcome message.
    This is useful for checking if the server is running.
    """
    return {"message": "Welcome to the FinsightAI API!"}


@app.get("/insight")
def get_stock_insight(ticker: str):
    """
    This is the core endpoint of our MVP. It takes a stock ticker,
    gathers the relevant data, uses an AI model to generate an insight,
    and returns it to the user.
    """
    # --- Step 1: Validate the Ticker (Basic Validation) ---
    if not ticker.endswith('.NS'):
        raise HTTPException(status_code=400, detail="Invalid ticker format. Ticker must end with '.NS'")

    # --- Step 2: Fetch Data from Knowledge Base (Live DB Logic) ---
    print(f"LOG: Received request for ticker: {ticker}. Fetching data from DB...")
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=503, detail="Database connection could not be established.")

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            # Get the security_id for the given ticker
            cursor.execute("SELECT id, long_name FROM securities WHERE ticker = %s;", (ticker,))
            security_record = cursor.fetchone()
            if not security_record:
                raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found in the database.")
            security_id = security_record['id']
            stock_name = security_record['long_name']

            # Fetch recent price data (last 7 days)
            seven_days_ago = datetime.now().date() - timedelta(days=7)
            cursor.execute(
                "SELECT trade_date, close_price FROM daily_prices WHERE security_id = %s AND trade_date >= %s ORDER BY trade_date DESC;",
                (security_id, seven_days_ago)
            )
            price_records = cursor.fetchall()
            # Format the price data into a simple string
            recent_prices = ", ".join([f"{rec['trade_date'].strftime('%b %d')}: ₹{rec['close_price']:.2f}" for rec in price_records])

            # Fetch recent news articles (last 5)
            cursor.execute(
                "SELECT title FROM news_articles WHERE security_id = %s ORDER BY published_at DESC LIMIT 5;",
                (security_id,)
            )
            news_records = cursor.fetchall()
            # Format the news data into a simple numbered list
            recent_news = " ".join([f"{i+1}. {rec['title']}" for i, rec in enumerate(news_records)])

    except HTTPException as e:
        raise e # Re-raise HTTPException to send proper error response
    except Exception as e:
        print(f"❌ DATABASE ERROR: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while fetching data from the knowledge base.")
    finally:
        if conn:
            conn.close()

    # --- Step 3: Generate Insight with LLM (Live AI Logic) ---
    print("LOG: Generating insight with local LLM...")
    ai_insight = ""
    try:
        # Construct the prompt for the LLM
        prompt = f"""
        You are an expert financial analyst for Indian retail investors. Your goal is to provide a concise, unbiased, and easy-to-understand insight based on the data provided. Do not give financial advice.

        Stock: {stock_name} ({ticker})

        Recent Price Action:
        {recent_prices}

        Recent News Headlines:
        {recent_news}

        Based on the data above, provide a 2-3 sentence summary of the current situation for this stock. Analyze the sentiment from the news and the trend from the price action.
        """

        # Call the local Ollama model
        response = ollama.chat(
            model='llama3:70b',
            messages=[{'role': 'user', 'content': prompt}]
        )
        ai_insight = response['message']['content']

    except Exception as e:
        print(f"❌ LLM ERROR: {e}")
        # Provide a fallback message if the LLM fails
        ai_insight = "The AI insight generator is currently unavailable. Please check the market data and news for your own analysis."


    # --- Step 4: Return the Result ---
    return {
        "ticker": ticker,
        "context": {
            "prices": recent_prices,
            "news": recent_news
        },
        "insight": ai_insight
    }


# This block allows you to run the API directly from the command line
if __name__ == "__main__":
    print("Starting FinsightAI API server...")
    uvicorn.run("main:main:app", host="127.0.0.1", port=8000, reload=True)
