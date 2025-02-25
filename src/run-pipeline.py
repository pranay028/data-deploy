import subprocess
import os
import sys


python_exe = sys.executable

def run_subprocess(script_path):
    
    try:
        result = subprocess.run([python_exe, "-m", script_path],check=True, capture_output=True,text=True)
        print("process completed succesfully")
        print("Captured output - ", result.stdout)
        
    except Exception as e:
        print("exception occured: ", e)
        print("Error details: ", e.stderr)
        sys.exit(1)
        

if __name__ == "__main__":
    
    print("strating the pipeline")
    
    print("Starting the scraper script")
    
    run_subprocess("src.scraper.data-collector")
    
    print("scraping finished successfully")
    
    print("starting the transform script")
    
    run_subprocess(r"src.transformers.data-transform")
    
    print("transformed data successfully")
    
    print("pipe line completed ")
    