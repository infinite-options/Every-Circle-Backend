from flask_restful import Resource
from flask import Flask, request, jsonify
import pandas as pd
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer
import pymysql
import os
from dotenv import load_dotenv
# import spacy
from data_ec import connect

load_dotenv()

# Load model only once, globally
model = SentenceTransformer("all-MiniLM-L6-v2")

class BusinessResults(Resource):
    def get(self, query):
        print("In Business Results")
        
    
        # user_query = request.args.get('query', type=str)
        user_query = query

        if not user_query:
            return {"message": "Missing query parameter"}, 400

        print('user_query:', user_query)

        
        # Read environment variables
        host = os.getenv("OPENSEARCH_HOST")
        port = int(os.getenv("OPENSEARCH_PORT"))
        username = os.getenv("OPENSEARCH_USERNAME")
        password = os.getenv("OPENSEARCH_PASSWORD")
        
        # Connect to OpenSearch
        client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=True
        )
        
        print("**** Connection established to openSearch ****")


         # Load NLP model
        # nlp = spacy.load("en_core_web_sm")

        # def extract_keywords(user_query: str):
        #     doc = nlp(user_query)
            
        #     # Extract nouns and proper nouns
        #     keywords = [token.lemma_.lower() for token in doc if token.pos_ in ['NOUN', 'PROPN'] 
        #                 and not token.is_stop]

        #     return keywords


        # keywords = extract_keywords(user_query)
        # in_keywords = str(", ".join(keywords))
        # #print('Extracted keywords from the input query', keywords)


         # Encode the user query to vector
        #model = SentenceTransformer("all-MiniLM-L6-v2")
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
            response = client.search(index="business", body=search_body)

            #print("response from openSearch", response)
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