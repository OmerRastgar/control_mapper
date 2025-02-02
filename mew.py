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

# Example usage:
if __name__ == "__main__":
    # Example 1: Using upload_common_control to create a common control node with multiple jurisdiction relationships.
    common_control_id = "common_control_1"
    # List of jurisdiction controls: each jurisdiction with its control number.
    jurisdiction_controls = [
        {"jurisdiction": "UK", "control_number": "Control 5"},
        {"jurisdiction": "Sweden", "control_number": "Control 6"}
    ]
    
    with driver.session() as session:
        # Upload common control with multiple jurisdictions
        common_control_result = session.execute_write(upload_common_control, common_control_id, jurisdiction_controls)
        if common_control_result:
            print("Common control node created/merged with multiple relationships:")
            print(common_control_result["c"])
        else:
            print("Failed to create/merge the common control with multiple jurisdictions.")

    # Example 2: Using upload_single_control to create or update a single jurisdiction relationship.
    with driver.session() as session:
        single_control_result = session.execute_write(upload_single_control, "common_control_2", "UK", "Control 7")
        single_control_result = session.execute_write(upload_single_control, "common_control_3", "UK", "Control 2")
        single_control_result = session.execute_write(upload_single_control, "common_control_4", "UK", "Control 8")
        single_control_result = session.execute_write(upload_single_control, "common_control_5", "Sweden", "Control 1")
        
    
    # Close the driver connection when done.
    driver.close()
