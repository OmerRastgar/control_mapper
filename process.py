import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

def get_xls_files(directory):
    """Retrieve all XLS/XLSX files from the given directory."""
    return [f for f in os.listdir(directory) if f.endswith(('.xls', '.xlsx'))]

def process_files(main_file, compare_files):
    """Simulate processing of main file with all other files."""
    print(f"Comparing main file: {main_file} with {len(compare_files)} other files.")
    for file in compare_files:
        print(f" - Comparing with: {file}")

def main(directory):
    files_list = get_xls_files(directory)
    
    if not files_list:
        print("No XLS files found in the directory.")
        return
    
    while files_list:
        main_file = files_list.pop(0)
        with ThreadPoolExecutor() as executor:
            executor.submit(process_files, main_file, files_list.copy())

if __name__ == "__main__":
    directory = r"C:\Users\Administrator\Downloads\control"  # Change to your target directory
    main(directory)
