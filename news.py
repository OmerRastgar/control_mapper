
import pandas as pd

def read_third_column(file_path):
    df = pd.read_excel(file_path, usecols=[2])  # Column index starts from 0, so 2 is the third column
    return df.iloc[:, 0].dropna().tolist()  # Convert to list, dropping any NaN values

# Example usage
file_path = "CIS.xlsx"  # Replace with your actual file path
third_column_data = read_third_column(file_path)

file_path2 = "iso.xlsx"  # Replace with your actual file path
third_column_data2 = read_third_column(file_path2)

from sentence_transformers import SentenceTransformer

# Load a model that supports large text embeddings
model = SentenceTransformer("BAAI/bge-large-en")

def get_embedding(text_list):
    """
    Takes a list of text (large paragraphs) and returns a list of embeddings.
    Each paragraph gets its corresponding vector.
    """
    return model.encode(text_list, normalize_embeddings=True).tolist()


embeddings = get_embedding(third_column_data)
embeddings2 = get_embedding(third_column_data2)



import torch
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

def find_similar_embeddings(list_a, list_b, threshold=0.85):
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



# Find similar embeddings
similar_indices = find_similar_embeddings(embeddings, embeddings2, threshold=0.85)
print(similar_indices)