# Define the directory paths
from datetime import datetime
import json
import os
import shutil
import pandas as pd
from dotenv import load_dotenv
from hpdb_module import hpdbCheck, hpdb_col
from kmz_module import kmzCheck, kmz_col
import time
import requests
import fnmatch
from colorama import Fore, Style
from ForceSys import formatted_print

text = '''
 __________  ____  ____________   
/ ____/ __ \/ __ \/ ____/ ____/   
/ /_  / / / / /_/ / /   / __/      
/ __/ / /_/ / _, _/ /___/ /___      
/_/    \____/_/ |_|\____/_____/      
'''

# Load environment variables from .env file
load_dotenv()

port=os.getenv("PORT")
force_base_url = os.getenv("FORCE_BASE_URL")
log_col = ['Cluster ID', 'Checking Date', 'Checking Time', "Status"]
log_location = '   MAIN'
text_color = Fore.BLUE

def main():
    print(Fore.BLUE + text + Style.RESET_ALL)
    time.sleep(11)
    print('\n')
    formatted_print(f'FORCE is Running on port {port}...', log_location, text_color)
    standby = False

    # Pola pencocokan nama file
    kmz_pattern = '*ABD*.kmz'

    while True:
        try:
            # Path direktori relatif
            dir_path = 'Summary/'

            # Mendapatkan waktu sekarang dalam detik
            now = time.time()

            # Melakukan iterasi pada setiap file dan direktori dalam dir_path
            for file_name in os.listdir(dir_path):
                if file_name != '.gitkeep':
                    file_path = os.path.join(dir_path, file_name)
                    # Memeriksa apakah file_path adalah file
                    if os.path.isfile(file_path):
                        # Mendapatkan waktu modifikasi file dalam detik
                        file_modified_time = os.path.getmtime(file_path)
                        # Memeriksa apakah waktu modifikasi lebih dari 7 hari yang lalu
                        if now - file_modified_time > 7 * 24 * 3600:
                            formatted_print(f'File {file_name} dimodifikasi lebih dari 7 hari yang lalu.', log_location, text_color)  
                            os.remove(file_path)
                            formatted_print(f'File {file_name} berhasil dihapus.', log_location, text_color)

            # Path direktori relatif
            dir_path = 'Input/'

            # Melakukan iterasi pada setiap file dan direktori dalam dir_path
            for file_name in os.listdir(dir_path):
                if file_name != '.gitkeep':
                    file_path = os.path.join(dir_path, file_name)
                    # Memeriksa apakah file_path adalah file
                    if os.path.isfile(file_path):
                        # Mendapatkan waktu modifikasi file dalam detik
                        file_modified_time = os.path.getmtime(file_path)
                        # Memeriksa apakah waktu modifikasi lebih dari 3 hari yang lalu
                        if now - file_modified_time > 3 * 24 * 3600:
                            formatted_print(f'File {file_name} dimodifikasi lebih dari 3 hari yang lalu.', log_location, text_color)  
                            os.remove(file_path)
                            formatted_print(f'File {file_name} berhasil dihapus.', log_location, text_color)

            url = f'{force_base_url}/get_downloaded_cluster_id'
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                if not data:  # Cek jika data kosong
                    if standby:
                        time.sleep(60)  # Jika data kosong, tunggu 60 detik sebelum cek lagi
                        continue
                    formatted_print('No cluster ID to Check. Stand by', log_location, text_color)
                    standby = True
                else:
                    standby = False
                    cluster_id = data[0]
                    with open('last_cluster_id.txt', 'w') as file:
                        file.write(str(cluster_id))
                    file_path = "Input\\" + cluster_id
                    formatted_print(f'Processing Cluster ID: {cluster_id}', log_location, text_color)
            else:
                formatted_print(f'Failed to get data. Status code: {response.status_code}', log_location, text_color)
            
            if not standby:

                input_dir = file_path
                files = os.listdir(input_dir)

                # Iterasi semua file dalam direktori
                for file in files:
                    if "HPDB" in file and "RPA" not in file and not file.startswith("~$"):
                        hpdb_file_path = os.path.join(input_dir, file)
                        formatted_print(f'HPDB File: {file}', log_location, text_color)
                    elif fnmatch.fnmatch(file, kmz_pattern):
                        kmz_file_path = os.path.join(input_dir, file)
                        formatted_print(f'KMZ File: {file}', log_location, text_color)
                    else:
                        continue

                start_time = time.time()

                # Get current date
                checking_date = datetime.today().strftime('%Y-%m-%d')
                checking_time = datetime.now().strftime('%H:%M:%S')

                summary_file_path = os.path.join("Summary", f"Checking Summary.csv")
                
                try:
                    hpdbCheck(hpdb_file_path, kmz_file_path, cluster_id, checking_date, checking_time, kmz_col, log_col)
                except:
                    df_error = pd.DataFrame({"Cluster ID": [cluster_id], "Error":"HPDB file error"})
                    with pd.ExcelWriter(summary_file_path, 'openpyxl', mode='a',  if_sheet_exists="overlay") as writer:
                    # fix line
                        reader = pd.read_excel(summary_file_path, sheet_name="Invalid Files")
                        df_error.to_excel(writer, sheet_name="Invalid Files", index=False, engine='openpyxl', header=False, startrow=len(reader)+1)

                try:        
                    kmzCheck(kmz_file_path, cluster_id, checking_date, checking_time, hpdb_col, log_col)
                except:
                    df_error = pd.DataFrame({"Cluster ID": [cluster_id], "Error":"KMZ file error"})
                    with pd.ExcelWriter(summary_file_path, 'openpyxl', mode='a',  if_sheet_exists="overlay") as writer:
                    # fix line
                        reader = pd.read_excel(summary_file_path, sheet_name="Invalid Files")
                        df_error.to_excel(writer, sheet_name="Invalid Files", index=False, engine='openpyxl', header=False, startrow=len(reader)+1)

                # Log the checking process
                url = f'{force_base_url}/update_processed'
                data = {
                    'cluster_id': cluster_id,
                    'processed' : 'TRUE'
                }
                headers = {'Content-Type': 'application/json'}
                response = requests.put(url, headers=headers, data=json.dumps(data))

                if response.status_code == 200:
                    formatted_print('Process status updated to TRUE', log_location, text_color)
                else:
                    formatted_print(f'Failed to update status. Status code: {response.status_code}', log_location, text_color)
                
                end_time = time.time()

                execution_time = end_time - start_time
                formatted_print(f'Execution time: {execution_time} seconds', log_location, text_color)

        except Exception as e:
            formatted_print(f'An error occurred: {e}', log_location, text_color)
            formatted_print('Skip to next Cluster ID in 5 seconds...', log_location, text_color)
            
            with open('last_cluster_id.txt', 'r') as file:
                cluster_id = str(file.read().strip())

            url = f'{force_base_url}/update_failed'
            data = {
                'cluster_id': cluster_id,
                'failed' : 'TRUE'
            }
            headers = {'Content-Type': 'application/json'}
            response = requests.put(url, headers=headers, data=json.dumps(data))

            time.sleep(5)

if __name__ == "__main__":
    main()