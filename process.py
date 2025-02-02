import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from sentence_transformers import SentenceTransformer



# Load a model that supports large text embeddings
model = SentenceTransformer("BAAI/bge-large-en")

def get_embedding(text_list):
    """
    Takes a list of text (large paragraphs) and returns a list of embeddings.
    Each paragraph gets its corresponding vector.
    """
    return model.encode(text_list, normalize_embeddings=True).tolist()


def read_third_column(file_path):
    df = pd.read_excel(file_path, usecols=[2])  # Column index starts from 0, so 2 is the third column
    return df.iloc[:, 0].dropna().tolist()  # Convert to list, dropping any NaN values




def get_xls_files(directory):
    """Retrieve all XLS/XLSX files from the given directory."""
    return [f for f in os.listdir(directory) if f.endswith(('.xls', '.xlsx'))]

def process_files(main_file, compare_files):
    mainfile_controls = read_third_column(main_file)
    mainfile_embedding = get_embedding(mainfile_controls) 

    for file in compare_files:
        file_controls = read_third_column(file)
        file_embedding = get_embedding(file_controls)
        similarity = calculate_similarity(mainfile_embedding, file_embedding)
        add_similarity_to_database(main_file, file, similarity)
        add_controls_to_database(file, file_controls)
        add_controls_to_database(main_file, mainfile_controls)



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
