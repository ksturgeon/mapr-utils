from mapr.ojai.storage.ConnectionFactory import ConnectionFactory
import json
import getpass


print("Non-secure cluster only.  Yelp Business Table\n")
host = raw_input("DAG host:")
username = raw_input("username [mapr]:")
if len(username) == 0:
  username="mapr"
password = getpass.getpass(prompt = "Password [maprmapr]:")
if len(password) == 0:
  password="maprmapr"
tbl_path = raw_input("Table path [/demo-tables/business]:")
if len(tbl_path) == 0:
  tbl_path="/demo-tables/business"
doc_id = raw_input("document to find [TGWhGNusxyMaA4kQVBNeew]")
if len(doc_id) == 0:
  doc_id="TGWhGNusxyMaA4kQVBNeew"


# Create a connection to data access server
connection_str = "{}:5678?auth=basic;user={};password={};ssl=false".format(host,username,password) 
connection = ConnectionFactory.get_connection(connection_str=connection_str)

# Get a store and assign it as a DocumentStore object
store = connection.get_store(tbl_path)

# fetch the OJAI Document by its '_id' field
doc = store.find_by_id(doc_id)

# Print the OJAI Document
print(json.dumps(doc))
#print(doc.as_dictionary())

# close the OJAI connection
connection.close()
