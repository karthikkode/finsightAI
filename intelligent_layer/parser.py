import pdfplumber
import logging
from typing import List
import os

class DocumentParser:
    """
    Handles parsing of downloaded documents (PDFs and TXTs), extracting text,
    and splitting it into manageable chunks for AI processing.
    """

    def __init__(self, chunk_size: int = 300, chunk_overlap: int = 50):
        """
        Initializes the parser with chunking parameters.

        Args:
            chunk_size (int): The target size of each text chunk in words.
            chunk_overlap (int): The number of words to overlap between chunks
                                 to maintain context.
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        logging.info(f"DocumentParser initialized with chunk size {chunk_size} and overlap {chunk_overlap}.")

    def parse_document(self, file_path: str) -> str:
        """
        Extracts clean text content from a given file (PDF or TXT).
        This is the main entry point for the parser.
        """
        if not os.path.exists(file_path):
            logging.error(f"File not found: {file_path}")
            return ""

        if file_path.lower().endswith('.pdf'):
            return self._parse_pdf(file_path)
        elif file_path.lower().endswith('.txt'):
            return self._parse_txt(file_path)
        else:
            logging.warning(f"Unsupported file type: {file_path}. Skipping.")
            return ""

    def _parse_pdf(self, pdf_path: str) -> str:
        """
        Extracts all clean text content from a given PDF file using pdfplumber.
        """
        logging.info(f"Parsing PDF: {pdf_path}")
        full_text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    # Extract text, handling potential None return for empty pages
                    page_text = page.extract_text()
                    if page_text:
                        full_text += page_text + "\n"
            
            # Clean up common text extraction issues like extra newlines and whitespace
            clean_text = ' '.join(full_text.split())
            logging.info(f"Successfully extracted {len(clean_text.split())} words from {pdf_path}.")
            return clean_text
        except Exception as e:
            logging.error(f"Failed to parse PDF {pdf_path}: {e}")
            return ""

    def _parse_txt(self, txt_path: str) -> str:
        """
        Reads and cleans text content from a .txt file.
        """
        logging.info(f"Parsing TXT: {txt_path}")
        try:
            with open(txt_path, 'r', encoding='utf-8') as f:
                full_text = f.read()
            
            clean_text = ' '.join(full_text.split())
            logging.info(f"Successfully read {len(clean_text.split())} words from {txt_path}.")
            return clean_text
        except Exception as e:
            logging.error(f"Failed to read TXT file {txt_path}: {e}")
            return ""

    def chunk_text(self, text: str) -> List[str]:
        """
        Splits a long piece of text into smaller, overlapping chunks.
        """
        if not text:
            return []
            
        words = text.split()
        chunks = []
        
        # Use a sliding window approach to create overlapping chunks
        start = 0
        while start < len(words):
            end = start + self.chunk_size
            chunk = words[start:end]
            chunks.append(" ".join(chunk))
            
            # Move the window forward, accounting for the overlap
            start += self.chunk_size - self.chunk_overlap
            
        logging.info(f"Split text into {len(chunks)} chunks.")
        return chunks

# --- Example of how to use this class for testing ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # You need to have a PDF file downloaded from the previous step to test this.
    # Make sure this file exists from your previous download run.
    sample_pdf_path = "financial_reports/annual_reports/RELIANCE_2024.pdf"
    
    parser = DocumentParser()
    
    # 1. Test document parsing
    extracted_text = parser.parse_document(sample_pdf_path)
    
    if extracted_text:
        # 2. Test text chunking
        text_chunks = parser.chunk_text(extracted_text)
        
        if text_chunks:
            print(f"\n--- Successfully created {len(text_chunks)} chunks ---")
            print("\n--- First Chunk Preview ---")
            print(text_chunks[0])
            print("\n--- Second Chunk Preview (showing overlap) ---")
            print(text_chunks[1])
