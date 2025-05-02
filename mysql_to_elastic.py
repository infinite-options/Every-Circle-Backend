import mysql.connector # type: ignore
from elasticsearch import Elasticsearch # type: ignore
import pandas as pd # type: ignore

# Connect to MySQL
mysql_conn = mysql.connector.connect(
    host="io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com",
    user="admin",
    password="prashant",
    database="every_circle"
)
# cursor = mysql_conn.cursor(dictionary=True)
# cursor.execute("SELECT * FROM ratings")
# rows = cursor.fetchall()
# df = pd.DataFrame(rows)

# # Connect to Elastic Cloud
# es = Elasticsearch(
#     cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
#     basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
# )
# # # Send each row to Elasticsearch
# for i, row in df.iterrows():
#     es.index(index="ratings", document=row.to_dict())

# print(f"✅ Done! Indexed {len(df)} records from MySQL to Elastic Cloud.")

###******** business ******###########
cursor = mysql_conn.cursor(dictionary=True)
cursor.execute("SELECT business_uid,business_uid,business_name,business_city,business_state FROM business")
rows = cursor.fetchall()
df = pd.DataFrame(rows)

# df.dropna
# Connect to Elastic Cloud
es = Elasticsearch(
    cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
    basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
)

# # Send each row to Elasticsearch
for i, row in df.iterrows():
    es.index(index="business", document=row.to_dict())

print(f"✅ Done! Indexed {len(df)} records from MySQL to Elastic Cloud.")

###******** business_category ******###########
# cursor = mysql_conn.cursor(dictionary=True)
# cursor.execute("SELECT * FROM business_category")
# rows = cursor.fetchall()
# df = pd.DataFrame(rows)

# # Connect to Elastic Cloud
# es = Elasticsearch(
#     cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
#     basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
# )

# # # Send each row to Elasticsearch
# for i, row in df.iterrows():
#     es.index(index="business_category", document=row.to_dict())

# print(f"✅ Done! Indexed {len(df)} records from MySQL to Elastic Cloud.")


###******** category ******###########
# cursor = mysql_conn.cursor(dictionary=True)
# cursor.execute("SELECT * FROM category")
# rows = cursor.fetchall()
# df = pd.DataFrame(rows)

# # Connect to Elastic Cloud
# es = Elasticsearch(
#     cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
#     basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
# )

# # # Send each row to Elasticsearch
# for i, row in df.iterrows():
#     es.index(index="category", document=row.to_dict())

# print(f"✅ Done! Indexed {len(df)} records from MySQL to Elastic Cloud.")


###******** business mapping ******###########
# cursor = mysql_conn.cursor(dictionary=True)
# cursor.execute("SELECT * FROM category")
# rows = cursor.fetchall()
# df = pd.DataFrame(rows)

# # Connect to Elastic Cloud
# es = Elasticsearch(
#     cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
#     basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
# )

# # # Send each row to Elasticsearch
# for i, row in df.iterrows():
#     es.index(index="category", document=row.to_dict())

# print(f"✅ Done! Indexed {len(df)} records from MySQL to Elastic Cloud.")


