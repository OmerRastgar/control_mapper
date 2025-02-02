from neo4j import GraphDatabase

# Replace with your credentials and connection info
uri = "neo4j+s://4df8dadc.databases.neo4j.io"
username = "neo4j"          # Change if needed
password = "Pe4RrdBrQ9LVBWShZbfUK5FIe39l7rWrNP51dWSMsrM"  # Replace with your actual password

# Create a driver instance to connect to the Neo4j database
driver = GraphDatabase.driver(uri, auth=(username, password))

def get_controls_by_jurisdictions(tx, jurisdictions):
    """
    Query the database for controls linked to the provided list of jurisdictions.
    If a control is linked to more than one jurisdiction, it is returned only once,
    along with the aggregated jurisdiction details.

    :param tx: The transaction object.
    :param jurisdictions: A list of jurisdiction names (e.g., ["UK", "Sweden"]).
    :return: A list of dictionaries, each representing a control and its associated jurisdictions.
    """
    query = """
    MATCH (j:Jurisdiction)-[r:CONNECT_TO]->(c:Control)
    WHERE j.name IN $jurisdictions
    WITH c, collect({jurisdiction: j.name, control_number: r.control_number}) AS jurisdictionDetails
    RETURN c.id AS control_id,
           c.description AS description,
           jurisdictionDetails
    ORDER BY c.id
    """
    result = tx.run(query, jurisdictions=jurisdictions)
    return [record.data() for record in result]

# Example usage:
if __name__ == "__main__":
    # Example: Query controls for one or more jurisdictions.
    # This list can have one or more jurisdiction names.
    jurisdictions_to_query = ["UK", "Sweden"]

    with driver.session() as session:
        controls = session.execute_read(get_controls_by_jurisdictions, jurisdictions_to_query)
        if controls:
            print("Combined controls for jurisdictions", jurisdictions_to_query)
            for control in controls:
                print("Control ID:", control["control_id"])
                if control["description"]:
                    print("Description:", control["description"])
                else:
                    print("Description: None")
                print("Jurisdiction Details:")
                for detail in control["jurisdictionDetails"]:
                    print("  - Jurisdiction: {}, Control Number: {}".format(
                        detail["jurisdiction"], detail["control_number"]))
                print("-" * 40)
        else:
            print("No controls found for the provided jurisdictions.")
        print ()
        print(controls)

    # Close the driver connection when done.
    driver.close()
