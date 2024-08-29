import datetime
import time
import requests
import json
import os
import psycopg2
from colorama import Fore, Style
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from ForceSys import formatted_print

#----------------------
# Get Approve API
#----------------------
log_location = 'GET APR'
text_color = Fore.LIGHTCYAN_EX

# Load environment variables from .env file
load_dotenv()

force_base_url = os.getenv("FORCE_BASE_URL")

def getToken(username, password):
    url = os.getenv("GET_TOKEN_URL")
    auth = os.getenv("GET_TOKEN_AUTH")
    content = os.getenv("GET_TOKEN_CONTENT")
    cookie = os.getenv("GET_TOKEN_COOKIE")
    headers = {
        'Authorization': auth,
        'Content-Type': content,
        'Cookie': cookie
    }

    data = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }

    # Make a POST request to the API endpoint with data and headers
    response = requests.post(url, data=data, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Print the response content (JSON, XML, or text)
        # print(response.json())  # If the response is JSON
        json_response = json.loads(response.text)  # If the response is plain text
        token = json_response["access_token"]
        # print(response.content)  # If you want the raw content
        # formatted_print('Get token succeed', log_location, text_color)
    else:
        # If the request was unsuccessful, print the status code
        formatted_print('Get token failed', log_location, text_color)

    return token

def getRequestApproval(token, startCreatedDate, endCreatedDate):
    url = os.getenv("GET_REQ_APR_URL")
    content = os.getenv("GET_REQ_APR_CONTENT")
    cookie = os.getenv("GET_REQ_APR_COOKIE")
    params = {
        'name': 'RFS Homepass',
        'startCreatedDate': startCreatedDate,
        'endCreatedDate': endCreatedDate,
        'sort': 'createdDate',
        'asc': '0',
    }

    headers = {
        'Content-Type': content,
        'Authorization': token,
        'Cookie': cookie
    }

    base_dir = os.path.join(os.getcwd(), "Input")

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        json_response = json.loads(response.text)
        contents = json_response["data"]["content"]
        net_contents = []
        net_contents_status = []
        net_contents_id = []
        for content in contents:
            if (content['status'] == 'On Progress' or content['status'] == 'Revise')  and content['statusAfter'] == 'groupApprovalLayer2':
                net_contents.append(content["description"].strip()) 
                net_contents_status.append(content['status'])
                net_contents_id.append(content["id"])
                if(content['status'] == 'Revise'):
                    print(content["description"].strip())
        
        response = requests.get(url)

        url = f'{force_base_url}/get_downloaded'
        response = requests.get(url)
        if response.status_code == 200:
            downloaded = response.json()
        else:
            formatted_print('Failed to get downloaded cluster IDs', log_location, text_color)        

        # force get all cluster ids
            
        for content, status in zip(net_contents, net_contents_status):
            url = f'{force_base_url}/get_all_cluster_ids'  
            response = requests.get(url)
            if response.status_code == 200:
                cluster_ids = response.json()
            else:
                formatted_print('Failed to get cluster IDs', log_location, text_color)

            if content not in cluster_ids:
                # force insert api
                url = f"{force_base_url}/insert_data"
                data = {
                    'cluster_id': content,
                    'processed': 'FALSE'
                }
                headers = {'Content-Type': 'application/json'}
                response = requests.post(url, headers=headers, data=json.dumps(data))

                if response.status_code == 200:
                    formatted_print(f'{content} inserted successfully.', log_location, text_color)
                else:
                    formatted_print(f'{content} insert failed.', log_location, text_color)
                
            elif content in cluster_ids and status == 'Revise':
                # force update process
                url = f'{force_base_url}/update_processed'
                data = {
                    'cluster_id': content,
                    'processed': 'FALSE'
                }
                headers = {'Content-Type': 'application/json'}
                response = requests.put(url, headers=headers, data=json.dumps(data))

                if response.status_code == 200:
                    formatted_print(f'{content} updated successfully.', log_location, text_color)
                else:
                    formatted_print(f'{content} update failed.', log_location, text_color)

                # force update failed
                url = f'{force_base_url}/update_failed'
                data = {
                    'cluster_id': content,
                    'failed': 'FALSE'
                }
                headers = {'Content-Type': 'application/json'}
                response = requests.put(url, headers=headers, data=json.dumps(data))

                if response.status_code == 200:
                    formatted_print(f'{content} updated successfully.', log_location, text_color)
                else:
                    formatted_print(f'{content} update failed.', log_location, text_color)

        for content, status, id in zip(net_contents, net_contents_status, net_contents_id):
            if status == 'Revise' or (status == 'On Progress' and (content not in downloaded or not downloaded)):
                if os.path.exists(os.path.join(base_dir, content)):
                    os.replace(os.path.join(base_dir, content), os.path.join(base_dir, content)) 
                else:
                    os.makedirs(os.path.join(base_dir, content))
                getDetailRequest(token, id, content)

    else:
        formatted_print(f'Document request failed. Response: {response.text}', log_location, text_color)

def getDetailRequest(token, id, cluster_id):
    url = str(os.getenv("GET_DETAIL_URL")) + id
    user = os.getenv("GET_DETAIL_USER")
    cookie = os.getenv("GET_DETAIL_COOKIE")
    headers = {
        'User-ID': user,
        'Authorization': token,
        'Cookie': cookie
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        json_response = json.loads(response.text)
        contents = json_response["data"]["object"]["fileDocuments"]
        for content in contents:
            if content["name"].endswith('.xlsx') and 'HPDB' in content["name"] and 'RPA' not in content["name"]:
                downloadReqApproval(token, cluster_id, content["name"], content["minioId"])
            elif content["name"].endswith('.kmz') and 'ABD' in content["name"]:
                downloadReqApproval(token, cluster_id, content["name"], content["minioId"])
    else:
        formatted_print('Detail request failed.', log_location, text_color)

    url = f"{force_base_url}/update_downloaded"
    data = {
        "cluster_id": cluster_id,
        "downloaded": "TRUE"
    }
    headers = {'Content-Type': 'application/json'}
    response = requests.put(url, headers=headers, data=json.dumps(data))

def downloadReqApproval(token, cluster_id, file_name, minioId):
    url = os.getenv("DOWNLOAD_REQ_APR_URL") + minioId
    user = os.getenv("DOWNLOAD_REQ_APR_USER")
    filename = os.path.join(cluster_id, file_name)  # Specify the filename for the downloaded file
    headers = {
        'User-ID': user,
        'Authorization': token,
    }
    base_dir = os.path.join(os.getcwd(), "Input")
    # Send a GET request to the URL
    response = requests.get(url, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the file in binary write mode
        with open(os.path.join(base_dir,filename), 'wb') as f:
            # Write the content of the response to the file
            f.write(response.content)
        formatted_print(f'{filename} downloaded successfully.', log_location, text_color)

    else:
        formatted_print(f'{filename} download failed.', log_location, text_color)

def getAppr_Controller():
    time.sleep(10) # Wait for 5 seconds to make sure the server is up
    user = os.getenv("GETAPPR_USER")
    password = os.getenv("GETAPPR_PASS")
    token = getToken(user, password)

    # Convert input strings to datetime objects
    # (dd-mm-yyyy) ex: "01-06-2024"
    start_date = "25-06-2024"
    end_date = "27-06-2024"

    strp_start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y")
    strp_end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y")
    
    # Loop from start date to end date
    current_date = strp_start_date
    while current_date < strp_end_date:
        next_date = current_date + datetime.timedelta(days=1)
        getRequestApproval(token, current_date.strftime('%d-%m-%Y'), next_date.strftime('%d-%m-%Y'))
        current_date = next_date
        
    formatted_print(f'All files have been downloaded. Will be back in 15 minutes...', log_location, text_color)
    time.sleep(900) # Wait for 15 minutes to get the next data

if __name__ == '__main__':
    while True:
        try:
            getAppr_Controller()
        except Exception as e:
            formatted_print(f'Something error happened. Error: {e}. Restarting in 15 sec...', log_location, text_color)
            time.sleep(15)
