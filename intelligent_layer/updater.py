import logging
import time
from tqdm import tqdm
import os
import re
from datetime import datetime
import concurrent.futures
import shutil

# Use the shared DatabaseManager from the root directory
from database import DatabaseManager
from .parser import DocumentParser
from .embedder import EmbeddingGenerator
from . import config

def process_single_file(file_path, db_manager, parser, embedder, ticker_to_id_map):
    """
    Processes a single document file: parses, chunks, embeds, and stores it.
    This function is designed to be run in a separate thread.
    """
    try:
        # --- 1. Extract Metadata from Filename ---
        filename = os.path.basename(file_path)
        parts = os.path.splitext(filename)[0].split('_')
        if len(parts) < 3:
            logging.warning(f"Skipping malformed filename: {filename}")
            return False

        doc_type_raw = parts[1]
        if doc_type_raw in ["AR", "Concall", "PPT"] and not filename.lower().endswith('.pdf'): return False
        if doc_type_raw == "CR" and not (filename.lower().endswith('.pdf') or filename.lower().endswith('.txt')): return False
        
        ticker_base = parts[0]
        ticker = f"{ticker_base}.NS"
        security_id = ticker_to_id_map.get(ticker)
        if not security_id: return False

        # --- 2. Parse, Chunk, and Embed ---
        raw_text = parser.parse_document(file_path)
        if not raw_text: return False
        
        text_chunks = parser.chunk_text(raw_text)
        if not text_chunks: return False

        # --- 3. Extract document type and date ---
        date_str = parts[-1]
        doc_type_parts = parts[1:-1]
        doc_type_map = {"AR": "Annual Report", "CR": "Credit Rating", "Concall": "Concall Transcript", "PPT": "Concall PPT"}
        doc_type = doc_type_map.get(doc_type_raw, " ".join(doc_type_parts))
        
        report_date = None
        try:
            if len(date_str) == 4: report_date = datetime.strptime(f"{date_str}-03-31", '%Y-%m-%d').date()
            elif len(date_str) == 8: report_date = datetime.strptime(date_str, '%Y%m%d').date()
            elif len(date_str) == 6: report_date = datetime.strptime(date_str, '%Y%m').date()
        except ValueError:
            logging.warning(f"Could not parse date from filename: {filename}")

        # --- 4. Store Chunks in DB ---
        for chunk in text_chunks:
            clean_chunk = chunk.replace('\x00', '')
            embedding = embedder.generate_embedding(clean_chunk)
            if not embedding: continue
            
            doc_info = {
                'security_id': security_id,
                'document_type': doc_type,
                'source_url': f"file://{file_path}",
                'report_date': report_date,
                'chunk_text': clean_chunk,
                'embedding': embedding
            }
            db_manager.upsert_document_chunk(doc_info)
        return True

    except Exception as e:
        logging.error(f"Failed to process file {filename}: {e}", exc_info=True)
        # --- Quarantine Logic ---
        quarantine_dir = os.path.join(config.SOURCE_DOCUMENTS_DIR, "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        shutil.move(file_path, os.path.join(quarantine_dir, filename))
        logging.warning(f"Moved problematic file to quarantine: {filename}")
        
        # --- Cleanup Logic ---
        db_manager.delete_chunks_for_file(f"file://{file_path}")
        return False

def run_document_processing():
    """
    Main orchestration function for processing documents using a thread pool.
    """
    logging.info("🚀 Starting document processing and indexing service...")
    
    db_manager = None
    try:
        db_manager = DatabaseManager()
        parser = DocumentParser()
        embedder = EmbeddingGenerator()
        
        tickers_info = db_manager.get_all_tickers()
        if not tickers_info:
            logging.warning("No tickers found in the database.")
            return

        ticker_to_id_map = {info['ticker']: info['id'] for info in tickers_info}
        
        # --- Collect all file paths first ---
        all_files_to_process = []
        for subdir, _, files in os.walk(config.SOURCE_DOCUMENTS_DIR):
            if "quarantine" in subdir: continue # Skip the quarantine directory
            for filename in files:
                all_files_to_process.append(os.path.join(subdir, filename))
        
        if not all_files_to_process:
            logging.info("No documents found to process.")
            return

        logging.info(f"Found {len(all_files_to_process)} total documents to process.")

        # --- Use a ThreadPoolExecutor for parallel processing ---
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            # Create a future for each file processing task
            future_to_file = {executor.submit(process_single_file, file, db_manager, parser, embedder, ticker_to_id_map): file for file in all_files_to_process}
            
            # Use tqdm to create a progress bar
            for future in tqdm(concurrent.futures.as_completed(future_to_file), total=len(all_files_to_process), desc="Processing Documents"):
                file = future_to_file[future]
                try:
                    future.result()
                except Exception as exc:
                    logging.error(f'{file} generated an exception: {exc}')

    except Exception as e:
        logging.critical(f"A critical error stopped the document processing: {e}", exc_info=True)
    
    finally:
        if db_manager and db_manager.pool:
            db_manager.pool.closeall()
            logging.info("Database connection pool closed.")
        
        logging.info("🎉 Document processing and indexing finished.")

if __name__ == '__main__':
    run_document_processing()
