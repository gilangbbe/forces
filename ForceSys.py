from datetime import datetime
import multiprocessing
from colorama import Fore, Style
import os

def force(script_name):
    os.system(f'python {script_name}')

def formatted_print(message, log_type, color):
    timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    log_type_colored = f"{color}{log_type}{Style.RESET_ALL}"
    formatted_message = f"{timestamp} --- [ {log_type_colored} ] {message}"
    print(formatted_message)

if __name__ == "__main__":
    scripts = ['getAppr_Controller.py', 'Backend.py', 'Main.py']
    # scripts = ['getAppr_Controller.py', 'Backend.py']
    # scripts = ['Backend.py', 'Main.py']
    # scripts = ['Backend.py']

    processes = []
    for script in scripts:
        process = multiprocessing.Process(target=force, args=(script,))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()
