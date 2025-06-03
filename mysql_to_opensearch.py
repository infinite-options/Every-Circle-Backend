import mysql.connector # type: ignore
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer

import pandas as pd # type: ignore

# Load the sentence transformer model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Connect to MySQL
mysql_conn = mysql.connector.connect(
    host="io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com",
    user="admin",
    password="prashant",
    database="every_circle"
)


###******** Extract Data from MySQL ******###########
cursor = mysql_conn.cursor(dictionary=True)
cursor.execute("SELECT business_uid,business_name FROM business")
rows = cursor.fetchall()
# df = pd.DataFrame(rows)

print('count fetched from backend',len(rows))

###******** # Transform data with embeddings *****###########
docs = []
for row in rows:
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
client = OpenSearch(
    hosts=[{'host': 'search-my-backend-lbghsfyddvevdy5kmj3hcbmbgq.aos.us-east-2.on.aws', 'port': 443}],
    http_auth=('Asharma@2', 'Asharma@2'),  # Use master user
    use_ssl=True,
    verify_certs=True
)

print(docs[:2])

# Bulk index
helpers.bulk(client, docs)
