from fastapi import FastAPI, HTTPException
import uvicorn
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import List
import json

# --- Project Structure Imports ---
from database import DatabaseManager
import ollama
from scheduler import scheduler, schedule_jobs

# --- Lifespan Management for Scheduler ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the application's startup and shutdown events.
    """
    print("INFO:     Starting up the application...")
    schedule_jobs()
    scheduler.start()
    yield
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


@app.get("/summarize-news")
def summarize_stock_news(ticker: str):
    """
    Fetches the latest news for a stock from the database, summarizes it,
    and provides a speculative analysis on market sentiment.
    """
    print(f"LOG: Received news summary request for {ticker}")
    db_manager = DatabaseManager()
    
    try:
        with db_manager.get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                # Find the security_id for the ticker
                cursor.execute("SELECT id, long_name FROM securities WHERE ticker = %s;", (ticker,))
                security_record = cursor.fetchone()
                if not security_record:
                    raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found.")
                security_id = security_record['id']
                stock_name = security_record['long_name']

                # Fetch the content of the 5 most recent news articles
                cursor.execute(
                    """
                    SELECT title, content FROM news_articles
                    WHERE security_id = %s
                    ORDER BY published_at DESC
                    LIMIT 5;
                    """,
                    (security_id,)
                )
                results = cursor.fetchall()
                if not results:
                    return {"summary": "No recent news articles found in the database for this stock."}

                # Combine all article content into a single context string
                context_str = "\n\n---\n\n".join([f"Title: {row['title']}\n\nContent: {row['content']}" for row in results])

        # --- Generate a summary and analysis using the powerful LLM ---
        print("LOG: Synthesizing news summary with the reasoning model...")
        prompt = f"""
        You are an expert financial analyst speaking to a beginner retail investor. Your goal is to provide a clear and simple summary.

        Analyze the following news articles for the company '{stock_name} ({ticker})'.

        News Articles:
        ---
        {context_str}
        ---

        Perform the following two tasks using simple, easy-to-understand language:
        1.  Provide a concise, 2-3 sentence summary of the key themes from these articles.
        2.  Based ONLY on the sentiment in these articles, provide a speculative analysis of the potential short-term market sentiment. Categorize it as 'Positive', 'Negative', or 'Neutral' and briefly explain why.

        IMPORTANT: Conclude with the disclaimer: "This is not financial advice."
        """

        response = ollama.chat(
            model='llama3:70b',
            messages=[{'role': 'user', 'content': prompt}]
        )
        final_answer = response['message']['content']

        return {"ticker": ticker, "analysis": final_answer}

    except Exception as e:
        print(f"❌ An error occurred during the news summary process: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while processing your request.")
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()


@app.get("/query")
def query_documents(ticker: str, question: str):
    """
    Answers a user's question about a stock by reasoning over the
    entire knowledge base of downloaded documents (Annual Reports, etc.).
    This is the main RAG (Retrieval-Augmented Generation) endpoint.
    """
    print(f"LOG: Received query for {ticker}: '{question}'")
    db_manager = DatabaseManager()
    
    try:
        # --- Step 1: Use AI to understand the user's query intent and extract filters ---
        print("LOG: Analyzing user query to extract filters and intent...")
        filter_prompt = f"""
        Analyze the user's question to understand their intent and extract any specific document types or years.
        Your response MUST be a JSON object with three keys: 'intent', 'document_type', and 'year'.
        The 'intent' can be 'specific_fact' or 'detailed_summary'.
        Valid document types are: 'Annual Report', 'Credit Rating', 'Concall Transcript', 'Concall PPT'.
        If the user asks for a "summary", "analysis", or "overview" of a document, the intent is 'detailed_summary'.
        If the user says "latest", the year should be the string "latest".
        Otherwise, the intent is 'specific_fact'.

        User Question: "{question}"

        Example 1: "What were the key risks in the 2023 annual report?" -> {{"intent": "specific_fact", "document_type": "Annual Report", "year": 2023}}
        Example 2: "Give me a detailed analysis of the latest annual report." -> {{"intent": "detailed_summary", "document_type": "Annual Report", "year": "latest"}}
        """
        response = ollama.chat(model='llama3:70b', messages=[{'role': 'user', 'content': filter_prompt}], format='json')
        query_details = json.loads(response['message']['content'])
        
        # --- Step 2: Generate an embedding for the user's question ---
        question_embedding = ollama.embeddings(model='mxbai-embed-large', prompt=question)['embedding']

        # --- Step 3: Retrieve relevant document chunks from the database ---
        with db_manager.get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute("SELECT id, long_name FROM securities WHERE ticker = %s;", (ticker,))
                security_record = cursor.fetchone()
                if not security_record:
                    raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found.")
                security_id = security_record['id']
                stock_name = security_record['long_name']

                # --- Build a dynamic SQL query with metadata filters ---
                query_params = [security_id, str(question_embedding)]
                sql_query = "SELECT chunk_text, document_type, report_date FROM document_chunks WHERE security_id = %s"
                
                if query_details.get('document_type'):
                    sql_query += " AND document_type = %s"
                    query_params.insert(1, query_details['document_type'])
                
                # --- FIX: Handle the 'year' filter more robustly ---
                year_filter = query_details.get('year')
                if isinstance(year_filter, int):
                    # User asked for a specific year
                    start_date = f"{year_filter}-01-01"
                    end_date = f"{year_filter}-12-31"
                    sql_query += " AND report_date BETWEEN %s AND %s"
                    query_params.insert(2, start_date)
                    query_params.insert(3, end_date)
                elif year_filter == 'latest':
                    # User asked for the "latest", so we don't filter by a specific year,
                    # but we will order by date later to find the newest document.
                    pass
                else:
                    # Default recency filter if no specific year is asked for
                    two_years_ago = datetime.now().date() - timedelta(days=730)
                    sql_query += " AND report_date >= %s"
                    query_params.insert(1, two_years_ago)
                
                # Adjust number of chunks based on intent
                num_chunks = 20 if query_details.get('intent') == 'detailed_summary' else 5
                # Add ordering to ensure we get the latest report when asked
                sql_query += f" ORDER BY report_date DESC, embedding <-> %s LIMIT {num_chunks};"
                
                cursor.execute(sql_query, tuple(query_params))
                results = cursor.fetchall()
                if not results:
                    return {"answer": "I could not find any relevant information in the specified documents to answer that question."}

                context_chunks = [row['chunk_text'] for row in results]
                context_str = "\n\n".join(context_chunks)
                source_info = f"This answer is based on information from the company's {results[0]['document_type']} dated {results[0]['report_date'].strftime('%Y-%m-%d')}."

        # --- Step 4: Generate a final answer using a dynamically chosen prompt ---
        print(f"LOG: Synthesizing final answer with intent: {query_details.get('intent')}")
        
        if query_details.get('intent') == 'detailed_summary':
            prompt = f"""
            You are an expert financial analyst providing a detailed summary for a beginner investor.
            Your task is to provide a structured, in-depth analysis of the provided document excerpts.
            
            User's Request: {question}
            Source: {source_info}
            Context from Document:
            ---
            {context_str}
            ---
            
            Based on the context, provide a detailed analysis covering the following sections:
            1.  **Key Financial Highlights:** Summarize the main financial performance points.
            2.  **Management's Outlook & Strategy:** What is the management's view on the future?
            3.  **Potential Risks & Concerns:** What are the key challenges or risks mentioned?
            
            Use simple language and explain any jargon.
            """
        else: # Default to specific_fact
            prompt = f"""
            You are an expert financial analyst explaining a concept to a beginner. Your task is to answer the user's question in simple, easy-to-understand terms.
            Base your answer on the context provided, but use your expert financial knowledge to interpret these facts and explain *why they matter*.
            If the context does not contain the answer, state that clearly. Do not give direct financial advice.
            Start your answer by stating the source of your information.

            User's Question: {question}
            Source: {source_info}
            Context from Documents:
            ---
            {context_str}
            ---
            """

        response = ollama.chat(
            model='llama3:70b',
            messages=[{'role': 'user', 'content': prompt}]
        )
        final_answer = response['message']['content']

        return {"ticker": ticker, "question": question, "answer": final_answer}

    except Exception as e:
        print(f"❌ An error occurred during the RAG process: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while processing your query.")
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
