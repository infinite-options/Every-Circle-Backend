from flask_restful import Resource
from flask import Flask, request, jsonify
import pandas as pd
from opensearchpy import OpenSearch, helpers, RequestsHttpConnection
from sentence_transformers import SentenceTransformer
import pymysql
import os
from dotenv import load_dotenv
from requests_aws4auth import AWS4Auth
import boto3

# import spacy
from data_ec import connect

load_dotenv()

# Load model only once, globally
model = SentenceTransformer("all-MiniLM-L6-v2")

# Read environment variables
host = os.getenv("OPENSEARCH_HOST", "localhost")
port = int(os.getenv("OPENSEARCH_PORT", "9200"))
region = os.getenv("REGION")
service =os.getenv("SERVICE")

class BusinessResults(Resource):
    def get(self, query):
        print("In Business Results")
        
    
        # user_query = request.args.get('query', type=str)
        user_query = query

        if not user_query:
            return {"message": "Missing query parameter"}, 400

        print('user_query:', user_query)

        # Get AWS credentials from environment or CLI
        session = boto3.Session(region_name=region)
        credentials = session.get_credentials()

        # print('session', session)
        # print('credentials', credentials)
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            region,
            service,
            session_token=credentials.token)

        # OpenSearch connection
        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection)

        # Encode the user query to vector
        query_vector = model.encode(user_query).tolist()
        # query_vector = model.encode(in_keywords).tolist()


        # print("**** query_vector ****", query_vector)

        # Define the search query for business data
        # Build KNN semantic search query
        search_body = {
                        "size": 10,
                        "_source": ["business_uid", "business_name"],
                        "query": {
                            "script_score": {
                                "query": {
                                    "match_all": {}
                                },
                                "script": {
                                    "source": "knn_score",
                                    "lang": "knn",
                                    "params": {
                                        "field": "business_name_vector",
                                        "query_value": query_vector,
                                        "space_type": "cosinesimil" 
                                    }
                                }
                            }
                        }
                    }


        #print('search_body', search_body)

        # Perform the search
        try:
            print("**** search_body ****", search_body)
            response = client.search(index="business", body=search_body)

            print("response from openSearch", response)
            hits = response["hits"]["hits"]

            if not hits:
                return {"message": "No results found"}, 404

            # Format response
            results = []
            for hit in hits:
                source = hit["_source"]
                score = hit["_score"]
                results.append({
                    "business_uid": source.get("business_uid"),
                    "business_name": source.get("business_name"),
                    "score": score
                })

            return jsonify({"results": results})

        except Exception as e:
            print(f"OpenSearch query error: {e}")
            return {"message": f"Internal Server Error: {str(e)}"}, 500