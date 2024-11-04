import pysolr 
import pandas as pd
import requests
import numpy as np

SOLR_URL = "http://localhost:8989/solr"
solr = pysolr.Solr(SOLR_URL)

def createCollection(collection_name):
    create_url = f"{SOLR_URL}/admin/collections?action=CREATE&name={collection_name}&numShards=1&replicationFactor=1"
    response = requests.get(create_url)
    if response.status_code == 200:
        print(f"Collection {collection_name} created successfully.")
    else:
        print(f"Failed to create collection {collection_name}: {response.text}")

def indexData(collection_name, exclude_column):
    df = None
    try:

        df = pd.read_csv("./Employee Sample Data 1.csv", encoding='ISO-8859-1')
        
        if exclude_column in df.columns:
            df = df.drop(columns=[exclude_column])
        else:
            print(f"Warning: Column '{exclude_column}' not found in the DataFrame. Skipping exclusion.")

        df.replace([np.inf, -np.inf], np.nan, inplace=True) 
        df.dropna(inplace=True)  
        data = df.to_dict(orient='records')

        if data:
            solr_url = f"{SOLR_URL}/{collection_name}/update/json/docs"
            response = requests.post(solr_url, json=data, headers={"Content-Type": "application/json"})
            if response.status_code == 200:
                print(f"Indexed {len(data)} records into {collection_name}.")
            else:
                print(f"Error indexing data: Solr responded with an error (HTTP {response.status_code}): {response.text}")
        else:
            print("No data to index.")
    except FileNotFoundError:
        print("Error: The specified CSV file was not found. Please check the file path.")
    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty.")
    except pd.errors.ParserError:
        print("Error: There was a problem parsing the CSV file.")
    except UnicodeDecodeError as e:
        print(f"Error: Unable to decode the CSV file: {e}. Try using a different encoding.")
    except Exception as e:
        print(f"Error indexing data: {e}")

def searchByColumn(collection_name, column_name, column_value):
    query = f"{column_name}:{column_value}"
    try:
        results = solr.search(query)
        return {'count': len(results), 'documents': results}
    except Exception as e:
        print(f"Error searching collection {collection_name}: {e}")
        return {'count': 0, 'documents': []}

def getEmpCount(collection_name):
    try:
        solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
        results = solr.search(":", rows=0) 
        return results.hits  
    except Exception as e:
        print(f"Error getting employee count: {e}")
        return 0

def delEmpById(collection_name, employee_id):
    try:
        solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
        solr.delete(id=employee_id)
        print(f"Employee with ID {employee_id} deleted successfully.")
    except Exception as e:
        print(f"Error deleting employee with ID {employee_id}: {e}")

def getDepFacet(collection_name):
    facet_query = {"facet": "on", "facet.field": "Department"}
    try:
        solr = pysolr.Solr(f'http://localhost:8989/solr/{collection_name}')
        results = solr.search(":", **facet_query)
        return results.facets
    except Exception as e:
        print(f"Error getting department facets: {e}")
        return {}

v_nameCollection = "Hash_Ranga"  
v_phoneCollection = "Hash_3139"  

createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# indexData(v_nameCollection, 'Department')
# indexData(v_phoneCollection, 'Gender')


# print("Employee count in Hash_Ranga:", getEmpCount(v_nameCollection))


# delEmpById(v_nameCollection, 'E02003')


# print("Employee count in Hash_Gowtham after deletion:", getEmpCount(v_nameCollection))


# print("Search by Department (IT):", searchByColumn(v_nameCollection, 'Department', 'IT'))
# print("Search by Gender (Male):", searchByColumn(v_nameCollection, 'Gender', 'Male'))
# print("Search by Department (IT) in Hash_0244:", searchByColumn(v_phoneCollection, 'Department', 'IT'))


# print("Department facets in Hash_Gowtham:", getDepFacet(v_nameCollection))
# print("Department facets in Hash_0244:", getDepFacet(v_phoneCollection))
