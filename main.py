from flask import Flask, request, jsonify, g
from typing import Dict, Any, Optional
import logging
from database_connection import DatabaseConnection, DatabaseType
from sql_agent import create_sql, execute_query
from schema_generation import SchemaExtractor
 

## configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

db_connection = None
engine = None
session = None
metadata = None


@app.route('/api/connect', methods=['POST'])
def create_connection():
    """Create a new database connection"""
    data = request.json
    global db_connection, engine, session, metadata
    try:
        db_type = DatabaseType(data['db_type'])
        if db_type == 'SQLITE':
            database = data['database']
            db_connection = DatabaseConnection(db_type, database)
            engine, session, metadata = db_connection.connect()

            # Store the connection info in the request context (g)
            return jsonify({"message": f"Successfully connected to the {db_type}"}), 200
        
        username = data['username']
        password = data['password']
        host = data['host']
        port = data['port']
        database = data['database']
        
        db_connection = DatabaseConnection(
            db_type,
            username,
            password,
            host,
            port,
            database)
        engine, session, metadata = db_connection.connect()
        # Store the connection info in the request context (g)
        return jsonify({"message": f"Successfully connected to the {db_type}"}), 200
    
    except Exception as e:
        logging.error(f"Error connecting to the database: {str(e)}")
        return jsonify({"erorr": str(e)}), 500
    
@app.route('/api/query', methods=['POST'])
def execute():
    """API to excute the natural language to the databse"""

    if not db_connection :
        return jsonify({"error": "Database connection not established. please call /connect first"}), 400
    
    db_connection.connect()
    
    
    data = request.json
    query = data['query']

    if not query:
        return jsonify({"error", "Missing Query"}), 400
    ## get schema of the databse
    try:
        schema = SchemaExtractor(metadata, engine)
        logging.info("Successfully load schema")
    except Exception as e:
        logging.error(f"Schema loading Error {e}")
    # convert natural language to sql
    try:
        sql_query = create_sql(
                    query,
                    schema.generate_rag_context(),
                    db_connection.db_type
                    )
        logging.info("Successfully created sql query")
    except Exception as e:
        logging.error(f"SQL query creation error {e}")
        return jsonify({"error": f"{str(e)}"})
    
    try:
        response = execute_query(sql_query, engine)
        logging.info("Successfully got the reponse")
    except Exception as e: 
        logging.error(f"Execution error {e}")
        return jsonify({"error": f"{str(e)}"})

    if response:
        print(response)
        return jsonify({"response": response}), 200
    

if __name__ == '__main__':
    app.run(debug=True)
        
       
            

        




    # Validate required fields




