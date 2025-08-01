import logging
import os
import datetime

LOG_FILE=f"{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.log"

logs_path=os.path.join(os.getcwd(),"logs",LOG_FILE)
os.makedirs(logs_path, exist_ok=True)
log_file_path = os.path.join(logs_path, LOG_FILE)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

