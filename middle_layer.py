from flask_restful import Resource
from flask import Flask, request, jsonify
import pandas as pd
from elasticsearch import Elasticsearch
# import mysql.connector # type: ignore
import pymysql
import os
from data_ec import connect

class Business_Results(Resource):
    def get(self, query):
        print("In Business Results")
        
        user_query = query

        # Connect to Elastic Cloud
        es = Elasticsearch(
        cloud_id="infinite_op:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyRmMDA2YWU3NjYwY2Y0OGMwYjViNDlkODQ1YTZjYTJlNSRjYjQyYTRlOGJlNDg0NjA2OGU4YjI5MmE5YzUxZWJlNA==",
        basic_auth=("elastic", "QqDaiHy53IyDU1mhCopcQF2M")
)

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
            threshold = 0.0
        
        print('Threshold list of business ids', threshold)

        # Filter top 10 business IDs meeting the threshold
        for b_id, score_val in business_results.items():
            if score_val >= threshold:
                top_ten_ids.append(b_id)
                if len(top_ten_ids) == 10:
                    break


        business_ids = ','.join(f'"{id_}"' for id_ in top_ten_ids)
        print('string business_ids', business_ids)

        response = {}
        try:
            with connect() as db:
                query_rating = f"SELECT b.*, r.* FROM business b LEFT JOIN ratings r  ON b.business_uid = r.rating_business_id WHERE b.business_uid in ({business_ids})"
                response = db.execute(query_rating, cmd='get')

            if not response['result']:
                response['message'] = f"No item found"
                response['code'] = 404
                return response, 404

            return response, 200

        except Exception as e:
            print(f"Error Middle Layer: {str(e)}")
            response['message'] = f"Internal Server Error: {str(e)}"
            response['code'] = 500
            return response, 500