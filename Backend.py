from datetime import datetime
import os
import time
from colorama import Fore, Style
import psycopg2
from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template, redirect, url_for
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
from ForceSys import formatted_print

#----------------------
# Force Database
#----------------------
    
# Load environment variables from .env file
load_dotenv()

# Create a PostgreSQL connection
conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

cur = conn.cursor()

# Initialize Flask app
app = Flask(__name__)

# Create a logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

# Add a file handler
file_handler = RotatingFileHandler('app.log', maxBytes=10240, backupCount=10)
file_handler.setLevel(logging.DEBUG)

# Create a custom formatter
class CustomFormatter(logging.Formatter):
    def format(self, record):
        log_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        log_level = record.levelname
        message = record.msg
        return f'{log_time} --- [{Fore.YELLOW} BACKEND {Style.RESET_ALL}] {message}'

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(CustomFormatter())

logger.addHandler(console_handler)

# Disable Werkzeug's default logging
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)
werkzeug_logger.addHandler(logging.NullHandler())

log_location = 'BACKEND'
text_color = Fore.YELLOW

#-----------------------------------
# Route to insert data into the database
#-----------------------------------
# Route to insert data into the database
@app.route('/insert_data', methods=['POST'])
def insert_data():
    data = request.json
    cluster_id = data.get('cluster_id')
    processed = data.get('processed')

    if not cluster_id or not processed:
        return jsonify({'error': 'Missing cluster_id or processed parameter'}), 400

    insert_data_in_db(conn, cluster_id, processed)
    logger.debug('POST /insert_data')
    return jsonify({'message': 'Data inserted successfully'}), 200

def insert_data_in_db(conn, cluster_id, processed):
    # Create a new cursor
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')

    # Execute the UPDATE statement
    cur.execute(
        "INSERT INTO logging (cluster_id, processed, last_edited) VALUES (%s, %s, CURRENT_TIMESTAMP)",
        (cluster_id, processed)
    )

    # Commit the changes to the database
    conn.commit()

    # Close the cursor
    cur.close()

#-----------------------------------
# Route to get list of clusters
#-----------------------------------
# Route to get list of clusters
@app.route('/clusters', methods=['GET'])
def clusters():
    return getClusterList()

def getClusterList():
    try:
        # Create a new cursor
        cur = conn.cursor()

        # Execute a query to select all clusters from the logging table
        cur.execute("SELECT DISTINCT cluster_id FROM logging")

        # Fetch all rows from the result set
        rows = cur.fetchall()

        # Close the cursor
        cur.close()

        # Return the list of clusters as a JSON response
        logger.debug('GET /clusters')
        return jsonify({"clusters": [row[0] for row in rows]})

    except psycopg2.Error as e:
        # Handle any errors that occur during database operation
        formatted_print(f"Error retrieving cluster list: {e}", log_location, text_color)
        return jsonify({"error": "Failed to retrieve cluster list"}), 500

#-----------------------------------
# Route to get data by cluster_id
#-----------------------------------
@app.route('/update_downloaded', methods=['PUT'])
def update_downloaded():
    data = request.json
    cluster_id = data.get('cluster_id')
    downloaded = data.get('downloaded')

    if not cluster_id or not downloaded:
        return jsonify({'error': 'Missing cluster_id or downloaded parameter'}), 400

    update_downloaded_in_db(conn, cluster_id, downloaded)
    logger.debug('PUT /update_downloaded')
    return jsonify({'message': 'Path updated successfully'}), 200

def update_downloaded_in_db(conn, cluster_id, downloaded):
    # Create a new cursor
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')

    # Execute the UPDATE statement
    cur.execute(
        "UPDATE logging SET downloaded = %s, last_edited = CURRENT_TIMESTAMP WHERE cluster_id = %s",
        (downloaded, cluster_id)
    )

    # Commit the changes to the database
    conn.commit()

    # Close the cursor
    cur.close()

#-----------------------------------
# Route to get data by cluster_id
#-----------------------------------
@app.route('/get_downloaded_cluster_id', methods=['GET'])
def get_downloaded_cluster_id():
    data = get_downloaded_cluster_id_from_db()
    logger.debug('GET /get_downloaded_cluster_id')
    return jsonify(data)

def get_downloaded_cluster_id_from_db():
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')

    cur.execute(
        "SELECT cluster_id FROM logging WHERE processed = 'FALSE' AND downloaded = 'TRUE' AND failed = 'FALSE' ORDER BY last_edited DESC LIMIT 1"
    )
    row = cur.fetchone()
    cur.close()
    return row

#-----------------------------------
# Route to update data on DB
#-----------------------------------

@app.route('/update_processed', methods=['PUT'])
def update_processed():
    data = request.json
    cluster_id = data.get('cluster_id')
    processed = data.get('processed')

    if not cluster_id or not processed:
        return jsonify({'error': 'Missing cluster_id or processed parameter'}), 400

    update_processed_in_db(conn, cluster_id, processed)
    logger.debug('PUT /update_processed')
    return jsonify({'message': 'Processed updated successfully'}), 200

def update_processed_in_db(conn, cluster_id, processed):
    # Create a new cursor
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')

    # Execute the UPDATE statement
    cur.execute(
        "UPDATE logging SET processed = %s, last_edited = CURRENT_TIMESTAMP WHERE cluster_id = %s", (processed, cluster_id)
    )

    # Commit the changes to the database
    conn.commit()

    # Close the cursor
    cur.close()

@app.route('/update_failed', methods=['PUT'])
def update_failed():
    data = request.json
    cluster_id = data.get('cluster_id')
    failed = data.get('failed')

    if not cluster_id or not failed:
        return jsonify({'error': 'Missing cluster_id or failed parameter'}), 400

    update_failed_in_db(conn, cluster_id, failed)
    logger.debug('PUT /update_failed')
    return jsonify({'message': 'failed updated successfully'}), 200

def update_failed_in_db(conn, cluster_id, failed):
    # Create a new cursor
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')

    # Execute the UPDATE statement
    cur.execute(
        "UPDATE logging SET failed = %s, last_edited = CURRENT_TIMESTAMP WHERE cluster_id = %s", (failed, cluster_id)
    )

    # Commit the changes to the database
    conn.commit()

    # Close the cursor
    cur.close()

#-----------------------------------
# Route to get all cluster ids
#-----------------------------------
@app.route('/get_all_cluster_ids', methods=['GET'])
def get_all_cluster_ids():
    cluster_ids = get_all_cluster_ids_from_db()
    logger.debug('GET /get_all_cluster_ids')
    return (cluster_ids)

def get_all_cluster_ids_from_db():
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')

    cur.execute("SELECT cluster_id FROM logging")
    rows = cur.fetchall()

    cur.close()

    cluster_ids = [row[0] for row in rows]
    return jsonify(cluster_ids)

@app.route('/get_downloaded', methods=['GET'])
def get_downloaded():
    cluster_ids = get_downloaded_from_db()
    logger.debug('GET /get_downloaded')
    return (cluster_ids)

def get_downloaded_from_db():
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')
        
    cur.execute("SELECT cluster_id FROM logging where downloaded = 'TRUE'")
    rows = cur.fetchall()

    cur.close()

    cluster_ids = [row[0] for row in rows]
    return jsonify(cluster_ids)

# Route to insert data into the database
@app.route('/reset_data', methods=['DELETE'])
def reset_data():

    reset_data_in_db(conn)
    logger.debug('DELETE /reset_data')
    return jsonify({'message': 'Data deleted successfully'}), 200

def reset_data_in_db(conn):
    # Create a new cursor
    try:
        cur = conn.cursor()
    except:
        logger.debug('Server is down.')

    # Execute the UPDATE statement
    cur.execute(
        "DELETE from logging where cluster_id is not null"
    )

    # Commit the changes to the database
    conn.commit()

    # Close the cursor
    cur.close()

# Define route for /CLUSTER1
@app.route('/summary/<cluster_id>')
def summary_page(cluster_id):
    summary_folder = os.path.join(os.path.dirname(__file__), 'Summary')
    csv_file_path = os.path.join(summary_folder, f'{cluster_id}.csv')
    static_folder = os.path.join(os.path.dirname(__file__), 'Static')
    logo_file_path = os.path.join(static_folder, 'Logo-slim.svg')

    if os.path.exists(csv_file_path):
        header = pd.read_csv(csv_file_path, nrows=0).columns.tolist()
    else:
        return redirect(url_for('page_not_found', cluster_id=cluster_id))
    formatted_header = tuple(header)
    data = pd.read_csv(csv_file_path, skiprows=1)
    formatted_data = [tuple(row) for row in data.values]
    return render_template('summary.html', headings=formatted_header, data=formatted_data, cluster_id=cluster_id)

@app.route('/summary', methods=['POST'])
def search():
    cluster_id = request.form['cluster_id']
    # Redirect to the endpoint with the cluster_id
    return redirect(url_for('summary_page', cluster_id=cluster_id))

@app.route('/page_not_found')
def page_not_found():
    # Redirect to the endpoint with the cluster_id
    return render_template('page_not_found.html')

@app.route('/')
def home_page():
    return render_template('index.html')

#-----------------------------------
# Main function to run the Flask app
#-----------------------------------
def backend():
    load_dotenv()
    time.sleep(5)
    port = os.getenv('PORT')
    host = os.getenv('IP')
    app.run(host=host, port=port, debug=True)
    # formatted_print(f'Running on {host}:{port}', log_location, text_color)


if __name__ == '__main__':
    while True:
        try:
            backend()
        except Exception as e:
            formatted_print(f'An error occurred: {e}', log_location, text_color)
            formatted_print('Restarting in 15 seconds...', log_location, text_color)
        time.sleep(15)