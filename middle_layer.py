# ************************************************************************************************************************
##### Simple approach 1: For the merged lists of business id getting the list of business ids and business name ######
# ************************************************************************************************************************

# import pandas as pd
# from elasticsearch import Elasticsearch

# # Connect to Elastic Cloud
# es = Elasticsearch(
#     cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
#     basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
# )

# # Define the search query for business data
# business_query = {
#     "_source": ["business_uid", "business_name"],
#     "query": {
#         "semantic": {
#             "query": "Cuisine",
#             "field": "business_name_vector"
#         }
#     }
# }

# # Define the search query for ratings data
# ratings_query = {
#     "_source": ["rating_business_id", "rating_description"],
#     "query": {
#         "semantic": {
#             "query": "Cuisine",
#             "field": "rating_desc_vector"
#         }
#     }
# }

# # Perform the multi-search query
# # es_response = es.msearch(body=[business_query, ratings_query])
# es_response = es.msearch(
#     body=[
#         { "index": "business_semantic" },  # The index metadata for the first search
#         business_query,  # The actual query body for business data
#         { "index": "ratings_semantic" },  # The index metadata for the second search
#         ratings_query  # The actual query body for ratings data
#     ]
# )

# print(es_response)

# # Extract business results
# business_results = [
#     {"business_uid": hit["_source"]["business_uid"]}
#     for hit in es_response["responses"][0]["hits"]["hits"]
# ]

# # Extract ratings results
# ratings_results = [
#     {"rating_business_id": hit["_source"]["rating_business_id"]}
#     for hit in es_response["responses"][1]["hits"]["hits"]
# ]

# # Step 2: Build dictionaries for quick lookup
# business_lists = [b["business_uid"] for b in business_results]
# ratings_lists = [r["rating_business_id"] for r in ratings_results]

# print("rating:", ratings_lists)
# print("business:",business_lists)

# # Step 3: Merge the results where business_uid == rating_business_id
# # merged_results = []
# # for business_uid in business_lists:
# #     if business_uid not in ratings_lists:
# #         merged_results.append(business_uid
# # )
# business_lists.extend(ratings_lists)

# business_lists = set(business_lists)

# print("merged", business_lists)
# # Convert to a DataFrame or print the merged results
# merged_df = pd.DataFrame(business_lists)
# print(merged_df)



# #search data in the elastic cloud
# # query = f"""
# # SELECT business_uid, business_name
# # FROM business
# # WHERE business_uid IN ({','.join(f'"{id_}"' for id_ in business_lists)})
# # """

# ec_query = f"""
# FROM business
# | WHERE business_uid IN ({','.join(f'"{str(id_)}"' for id_ in business_lists)})
# | KEEP business_uid, business_name
# """

# print(ec_query)

# response = es.esql.query(query=ec_query)
# print(response)




##### Approach 2: ids and score ######
# import pandas as pd
# from elasticsearch import Elasticsearch
# import mysql.connector # type: ignore


# # Connect to Elastic Cloud
# es = Elasticsearch(
#     cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
#     basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
# )

# # Define the search query for business data
# business_query = {
#     "_source": ["business_uid", "business_name"],
#     "query": {
#         "semantic": {
#             "query": "Cuisine",
#             "field": "business_name_vector"
#         }
#     }
# }

# # Define the search query for ratings data
# ratings_query = {
#     "_source": ["rating_business_id", "rating_description"],
#     "query": {
#         "semantic": {
#             "query": "Cuisine",
#             "field": "rating_desc_vector"
#         }
#     }
# }

# # Perform the multi-search query
# # es_response = es.msearch(body=[business_query, ratings_query])
# es_response = es.msearch(
#     body=[
#         { "index": "business_semantic" },  # The index metadata for the first search
#         business_query,  # The actual query body for business data
#         { "index": "ratings_semantic" },  # The index metadata for the second search
#         ratings_query  # The actual query body for ratings data
#     ]
# )

# print(es_response)


# # Extract business results
# business_results = {}
# for hit in es_response["responses"][0]["hits"]["hits"]:
#     business_uid = hit["_source"]["business_uid"]
#     score_b = hit["_score"]
#     business_results[business_uid] = score_b

# # business_results = [
# #     {"business_uid": hit["_source"]["business_uid"]}
# #     for hit in es_response["responses"][0]["hits"]["hits"]
# # ]

# print(business_results)

# # Extract ratings results
# # ratings_results = [
# #     {"rating_business_id": hit["_source"]["rating_business_id"]}
# #     for hit in es_response["responses"][1]["hits"]["hits"]
# # ]
# ratings_results = {}
# for hit in es_response["responses"][1]["hits"]["hits"]:
#     rating_business_id = hit["_source"]["rating_business_id"]
#     score_r = hit["_score"]
#     ratings_results[rating_business_id] = score_r


# print(ratings_results)



# for key, val in ratings_results.items():
#     if key in business_results:
#         business_results[key] += val
#     elif key not in business_results:
#         business_results[key] = val

# print("combined results:", business_results)

# # Sorting the results based on the score
# business_results = {k: float(v) for k, v in business_results.items()}

# print("Origianl result", business_results)

# business_results = sorted(business_results.items(), key=lambda item: item[1], reverse=True)

# print("sorted result:", business_results)

# top_ten_ids = [k for k, v in business_results][:10]

# print("Top 10 sorted combined results:", top_ten_ids)

# business_ids = ','.join(f'"{id_}"' for id_ in top_ten_ids)

# print('string business_ids', business_ids)

# ## getting the result from the elastic cloud:
# # ec_query = f"""
# # FROM business
# # | WHERE business_uid IN ({','.join(f'"{id_}"' for id_ in list(business_results.keys()))})
# # | KEEP business_uid, business_name
# # """

# # print(ec_query)

# # response = es.esql.query(query=ec_query)
# # print(response)


# # Connect to MySQL
# mysql_conn = mysql.connector.connect(
#     host="io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com",
#     user="admin",
#     password="prashant",
#     database="every_circle"
# )


# cursor = mysql_conn.cursor(dictionary=True)

# query_rating = f"SELECT b.*,r.* FROM business b INNER JOIN ratings r  ON b.business_uid = r.rating_business_id WHERE b.business_uid in ({business_ids}) AND r.rating_business_id in ({business_ids})"


# cursor.execute(query_rating)
# rows = cursor.fetchall()
# df = pd.DataFrame(rows)

# data = df.to_dict(orient='records') 

# # for i, row in df.iterrows():
# #     business_id = row["rating_business_id"]
# #     rating = row["rating_star"]
    
# #     if business_id in business_results:
# #         #score = business_results[business_id] if isinstance(business_results[business_id], (int, float)) else business_results[business_id]["score"]
        
# #         score = business_results[business_id]

# #         business_results[business_id] = {
# #             "score": score,
# #             "rating": rating
# #         }

# # print("New combined results with rating", business_results)


# from flask import Flask, jsonify
# app = Flask(__name__)


# @app.route('/api/business_results', methods=['GET'])
# def get_business_results():
#     return jsonify(data)


# if __name__ == '__main__':
#     app.run(debug=True)




# ######### Approach 3 ########
import pandas as pd
from elasticsearch import Elasticsearch
import mysql.connector # type: ignore
from flask import Flask, jsonify, request


# Connect to Elastic Cloud
es = Elasticsearch(
    cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
    basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
)

# Initialize Flask app
app = Flask(__name__)


@app.route('/api/business_results', methods=['GET'])
def get_business_results():

    user_query = request.args.get('query', type=str)

    # Define the search query for business data
    business_query = {
        "_source": ["business_uid", "business_name"],
        "query": {
            "semantic": {
                "query": user_query,
                "field": "business_name_vector"
            }
        }
    }

    # Define the search query for ratings data
    ratings_query = {
        "_source": ["rating_business_id", "rating_description"],
        "query": {
            "semantic": {
                "query": user_query,
                "field": "rating_desc_vector"
            }
        }
    }

    # Perform the multi-search query
    es_response = es.msearch(
        body=[
            { "index": "business_semantic" },  # The index metadata for the first search
            business_query,  # The actual query body for business data
            { "index": "ratings_semantic" },  # The index metadata for the second search
            ratings_query  # The actual query body for ratings data
        ]
    )

    print(es_response)

    # Extract business results
    business_results = {}
    for hit in es_response["responses"][0]["hits"]["hits"]:
        business_uid = hit["_source"]["business_uid"]
        score_b = hit["_score"]
        business_results[business_uid] = score_b

    print(business_results)

    # Extract rating results
    ratings_results = {}
    for hit in es_response["responses"][1]["hits"]["hits"]:
        rating_business_id = hit["_source"]["rating_business_id"]
        score_r = hit["_score"]
        ratings_results[rating_business_id] = score_r

    print(ratings_results)


    # Adding the scores of both business and rating
    for key, val in ratings_results.items():
        if key in business_results:
            business_results[key] += val
        elif key not in business_results:
            business_results[key] = val

    
    business_results = {k: float(v) for k, v in business_results.items()}
    print("Origianl result", business_results)

    # Sorting the results based on the score
    business_results = dict(sorted(business_results.items(), key=lambda item: item[1], reverse=True))
    print("sorted result:", business_results)

    # top_ten_ids = [k for k, v in business_results][:10]
    # print("Top 10 sorted combined results:", top_ten_ids)

    ## Filtering the result based on the scores
    top_ten_ids = []
    score_three_count = 0
    score_two_count = 0
    score_one_count = 0

    score_three_count =  sum(1 for v in business_results.values() if v >= 3.0)
    score_two_count   = sum(1 for v in business_results.values() if 2.0 <= v < 3.0)
    score_one_count   = sum(1 for v in business_results.values() if 1.0 <= v < 2.0)

    # Set threshold based on the highest available score tier
    if score_three_count > 0:
        threshold = 3.0
    elif score_two_count > 0:
        threshold = 2.0
    elif score_one_count > 0:
        threshold = 1.0
    else:
        threshold = float("-Inf")
    
    print('Threshold list of business ids', threshold)

    # Filter top 10 business IDs meeting the threshold
    for b_id, score_val in business_results.items():
        if score_val >= threshold:
            top_ten_ids.append(b_id)
            if len(top_ten_ids) == 10:
                break


    business_ids = ','.join(f'"{id_}"' for id_ in top_ten_ids)
    print('string business_ids', business_ids)

    # Connect to MySQL
    mysql_conn = mysql.connector.connect(
        host="io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com",
        user="admin",
        password="prashant",
        database="every_circle"
    )


    cursor = mysql_conn.cursor(dictionary=True)

    query_rating = f"SELECT b.*, r.* FROM business b LEFT JOIN ratings r  ON b.business_uid = r.rating_business_id WHERE b.business_uid in ({business_ids})"


    cursor.execute(query_rating)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)

    data = df.to_dict(orient='records') 

    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)