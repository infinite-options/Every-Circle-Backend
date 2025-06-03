from flask_restful import Resource
from flask import Flask, request, jsonify
import pandas as pd
from opensearchpy import OpenSearch, helpers
from sentence_transformers import SentenceTransformer
import pymysql
import os
import spacy
from data_ec import connect

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

        # Connect to OpenSearch
        client = OpenSearch(
            hosts=[{'host': 'search-my-backend-lbghsfyddvevdy5kmj3hcbmbgq.aos.us-east-2.on.aws', 'port': 443}],
            http_auth=('Asharma@2', 'Asharma@2'),  # Use master user
            use_ssl=True,
            verify_certs=True
        )

        print("**** Connection established to openSearch ****")
         # Load NLP model
        nlp = spacy.load("en_core_web_sm")

        def extract_keywords(user_query: str):
            doc = nlp(user_query)
            
            # Extract nouns and proper nouns
            keywords = [token.lemma_.lower() for token in doc if token.pos_ in ['NOUN', 'PROPN'] 
                        and not token.is_stop]

            return keywords


        keywords = extract_keywords(user_query)
        in_keywords = str(", ".join(keywords))
        #print('Extracted keywords from the input query', keywords)


         # Encode the user query to vector
        #model = SentenceTransformer("all-MiniLM-L6-v2")
        query_vector = model.encode(user_query).tolist()
        # query_vector = model.encode(in_keywords).tolist()


        print("**** query_vector ****", query_vector)

        # Define the search query for business data
        # Build KNN semantic search query
        # search_body = {
        #     "size": 10,
        #     "_source": ["business_uid", "business_name"],
        #     "knn": {
        #         "field": "business_name_vector",
        #         "query_vector": query_vector,
        #         "k": 10,
        #         "num_candidates": 100
        #     }
        # }

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
            # response = client.transport.perform_request(
            #             method="POST",
            #             url="/business/_knn_search",
            #             body=search_body
            #         )


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