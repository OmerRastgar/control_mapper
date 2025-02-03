import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from neo4j import GraphDatabase

# Replace with your credentials and connection info
uri = "neo4j+s://4df8dadc.databases.neo4j.io"
username = "neo4j"          # Change if needed
password = "Pe4RrdBrQ9LVBWShZbfUK5FIe39l7rWrNP51dWSMsrM"  # Replace with your actual password

driver = GraphDatabase.driver(uri, auth=(username, password))

def upload_common_control(tx, control_id, jurisdiction_controls):
    """
    Upload a common control node and create relationships for multiple jurisdictions.
    
    :param tx: The transaction object.
    :param control_id: Unique identifier for the common control (e.g., "common_control_1").
    :param jurisdiction_controls: A list of dictionaries, each with keys:
           - 'jurisdiction': The name of the jurisdiction (e.g., "UK")
           - 'control_number': The control number for that jurisdiction (e.g., "Control 5")
    :return: The common control node.
    """
    query = """
    MERGE (c:Control {id: $control_id})
    WITH c
    UNWIND $jurisdiction_controls AS item
      MERGE (j:Jurisdiction {name: item.jurisdiction})
      MERGE (j)-[r:CONNECT_TO]->(c)
      SET r.control_number = item.control_number
    RETURN c
    """
    result = tx.run(query, control_id=control_id, jurisdiction_controls=jurisdiction_controls)
    return result.single()

def upload_single_control(tx, control_id, jurisdiction, control_number):
    """
    Upload (or merge) a common control node and create a relationship for a single jurisdiction.
    
    :param tx: The transaction object.
    :param control_id: Unique identifier for the common control node.
    :param jurisdiction: The jurisdiction name (e.g., "UK").
    :param control_number: The control number for that jurisdiction (e.g., "Control 5").
    :return: The created or matched nodes and relationship.
    """
    query = """
    MERGE (c:Control {id: $control_id})
    MERGE (j:Jurisdiction {name: $jurisdiction})
    MERGE (j)-[r:CONNECT_TO]->(c)
    SET r.control_number = $control_number
    RETURN j, r, c
    """
    result = tx.run(query,
                    control_id=control_id,
                    jurisdiction=jurisdiction,
                    control_number=control_number)
    return result.single()





def calculate_similarity(list_a, list_b, threshold=0.85):
    """
    Compares embeddings from two lists and returns index pairs of similar ones.

    Args:
        list_a (list): First list of embeddings.
        list_b (list): Second list of embeddings.
        threshold (float): Cosine similarity threshold for considering embeddings similar.

    Returns:
        list: List of index pairs (i, j) where similarity > threshold.
    """
    # Convert to numpy arrays
    embeddings_a = np.array(list_a)
    embeddings_b = np.array(list_b)

    # Compute cosine similarity matrix
    similarity_matrix = cosine_similarity(embeddings_a, embeddings_b)

    # Find pairs above the threshold
    similar_pairs = [(i, j) for i in range(len(list_a)) for j in range(len(list_b)) 
                     if similarity_matrix[i, j] > threshold]

    return similar_pairs

def add_similarity_to_database(mainfile_controls,main_file, controlfile, similarity):
    main_file = main_file.replace(".xlsx", "")
    controlfile = controlfile.replace(".xlsx", "")
    jursidiction_controls = [
            {
                "control_number": 5,
                "jurisdiction": "mainfile"
            },
            {
                "control_number": 6,
                "jurisdiction": "control_file"
            }
        ]
    with driver.session() as session:
        for i, j in similarity:
            jursidiction_controls[0]["control_number"] = i
            jursidiction_controls[0]["jurisdiction"] = main_file
            jursidiction_controls[1]["control_number"] = j
            jursidiction_controls[1]["jurisdiction"] = controlfile
            session.execute_write(upload_common_control, mainfile_controls[i], jursidiction_controls)
    return 0
            
def add_controls_to_database(file, file_controls):
    file = file.replace(".xlsx", "")
    with driver.session() as session:
        for num , control in enumerate(file_controls):
            session.execute_write(upload_single_control, control, file,num )
    return 0


            
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
        similarity = calculate_similarity(mainfile_embedding, file_embedding,0.85)
        add_similarity_to_database(mainfile_controls, main_file, file, similarity)
        add_controls_to_database(file, file_controls)
        add_controls_to_database(main_file, mainfile_controls)



def main(directory):
    files_list = get_xls_files(directory)
    
    if not files_list:
        print("No XLS files found in the directory.")
        return
    
    while files_list:
        main_file = files_list.pop(0)
        process_files(main_file, files_list)

if __name__ == "__main__":
    directory = r"C:\Users\Administrator\Downloads\control"  # Change to your target directory
    main(directory)
