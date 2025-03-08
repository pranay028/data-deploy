import subprocess
import os
import sys
from .utils.logs import logger
from .utils.log_config import upload_log_to_s3

python_exe = sys.executable

def run_subprocess(script_path):
    """Runs a Python module as a subprocess and logs the output."""
    try:
        result = subprocess.Popen(
            [python_exe, "-m", script_path],
            check=True, text=True
        )
        logger.info(f"Process completed successfully")
        

    except subprocess.CalledProcessError as e:
        logger.error("Exception occurred while running %s: %s", script_path, str(e))
        logger.error(f"Error details \n: {e.stderr}")

if __name__ == "__main__":
    
    logger.info("Starting the pipeline")

    try:
        
        logger.info("Running scraper script")
        run_subprocess("src.scraper.data-collector")

        logger.info("Running transform script")
        run_subprocess("src.transformers.data-transform")

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")

    finally:
        
        upload_log_to_s3()
        
        
