import pymysql
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer
import os
from dotenv import load_dotenv

import pandas as pd # type: ignore

load_dotenv()

# Load the sentence transformer model
model = SentenceTransformer('all-MiniLM-L6-v2')

 # Read environment variables for database connection
host_info = os.getenv("RDS_HOST")
port_info = int(os.getenv("RDS_PORT"))
db = os.getenv("RDS_DB")
pwd = os.getenv("RDS_PW")
user_role = os.getenv("RDS_USER")

# Connect to MySQL using PyMySQL
mysql_conn = pymysql.connect(
    host=host_info,
    user=user_role,
    password=pwd,
    database=db,
    port=port_info,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)


###******** Extract Data from MySQL ******###########
cursor = mysql_conn.cursor(dictionary=True)
cursor.execute("SELECT business_uid,business_name FROM business")
rows = cursor.fetchall()
# df = pd.DataFrame(rows)

print('count fetched from backend',len(rows))

###******** # Transform data with embeddings *****###########
docs = []
for i, row in enumerate(rows):
    if i % 2 == 0:  # Print every 50 businesses
        print(f"Processing business {i+1}/{len(rows)}")
    embedding = model.encode(row["business_name"]).tolist()
    doc = {
        "_index": "business",
        "_id": row["business_uid"],
        "_source": {
            "business_uid": row["business_uid"],
            "business_name": row["business_name"],
            "business_name_vector": embedding
        }
    }
    docs.append(doc)


###******** Index to OpenSearch *****###########
 # Read environment variables
host = os.getenv("OPENSEARCH_HOST", "localhost")
port = int(os.getenv("OPENSEARCH_PORT", "9200"))
# username = os.getenv("OPENSEARCH_USERNAME")
# password = os.getenv("OPENSEARCH_PASSWORD")


# # Connect to OpenSearch
# client = OpenSearch(
#     hosts=[{'host': host, 'port': port}],
#     http_auth=(username, password),
#     use_ssl=True,
#     verify_certs=True
# )

# Connect to OpenSearch
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    use_ssl=False,
    verify_certs=False
)

print("**** Connection established to openSearch ****")
# print(docs[:2])

# Bulk index
helpers.bulk(client, docs)
