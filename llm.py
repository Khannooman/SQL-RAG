from langchain_groq import ChatGroq
from dotenv import load_dotenv
import os
import logging

load_dotenv(".env")

def llm():
    try:
        model = ChatGroq(
            model=os.getenv('model'),
            temperature=os.getenv('temperature')
        )
        logging.info(f"Successfully Initialize th mode {os.getenv('model')}")
        return model
        
    except Exception as e:
        logging.error(f"model Initialization error {e}")
        