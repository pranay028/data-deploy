import logging
import os
from datetime import datetime


LOG_FILE = f"etl_pipeline_{datetime.now().strftime(r'%Y-%m-%d_%H:%M:%S')}.log"
LOCAL_LOG_PATH = os.path.join(os.getcwd(), LOG_FILE)

logging.basicConfig(
    filename=LOCAL_LOG_PATH,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)
