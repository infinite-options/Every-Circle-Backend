from flask_restful import Resource
from flask import Flask, request, jsonify
import pandas as pd
from opensearchpy import OpenSearch, helpers, RequestsHttpConnection
from sentence_transformers import SentenceTransformer
from tagSearch_direct import TagSearchDirect
from tagCategorySearch import TagCategorySearch
import pymysql
import os
from dotenv import load_dotenv

# import spacy
from data_ec import connect

load_dotenv()

# Load model only once, globally
model = SentenceTransformer("all-MiniLM-L6-v2")

# Read environment variables
host = os.getenv("OPENSEARCH_HOST")
port = int(os.getenv("OPENSEARCH_PORT"))

class BusinessResults(Resource):
    def get(self, query):
        print("In Business Results")
        
    
        # user_query = request.args.get('query', type=str)
        user_query = query

        if not user_query:
            return {"message": "Missing query parameter"}, 400

        print('user_query:', user_query)

        # print("Checking")
        # print('host:', host, ' port:', port)

        client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        use_ssl=False,
        verify_certs=False,
        scheme="http",
        connection_class=RequestsHttpConnection)

        print('info', client)

        # Encode the user query to vector
        query_vector = model.encode(user_query).tolist()

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
                # return {"message": "No results found"}, 404
                raise ValueError("No results found") 

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

            return jsonify({"message": "Retrieved OpenSearch results",
                            "results": results})

        except Exception as e:
            print(f"OpenSearch query error: {e}")
            #return {"message": f"Internal Server Error: {str(e)}"}, 500
            print("Trying fallback TagSearchDirect")

            try:
                fallback = TagSearchDirect()
                fallback_res= fallback.get(query)
                print('fallback_res', type(fallback_res))

                if isinstance(fallback_res, tuple):
                    data, status = fallback_res
                    if status == 404:
                        raise ValueError("Fallback TagSearchDirect -No results found")
                
                results = fallback_res[0]['result']
                return jsonify({"message": "Retrieved TagSearchDirect results",
                                "results": results})
            
            except Exception as e2:
                print(f"Fallback TagSearchDirect failed: {e2}")
                # return {"message": "Both primary and fallback TagSearchDirect failed"}, 502

                print("Trying fallback TagCategorySearch")
                try:
                    fallback_2 = TagCategorySearch()
                    fallback_2res= fallback_2.get(query)
                    
                    if isinstance(fallback_2res, tuple):
                        data, status = fallback_2res
                        if status == 404:
                            msg = "Fallback TagCategorySearch -No results found"
                            raise ValueError(msg)
                
                    results = fallback_2res[0]['result']
                    return jsonify({"message": "Retrieved TagCategorySearch results",
                                    "results": results})
                
                except Exception as ValueError:
                    return {"message": msg}, 404
                
                except Exception as e3:
                    print(f"Fallback TagCategorySearch failed: {e3}")
                    # return {"message": "Both primary and fallback TagSearchDirect failed"}, 502
                    print("Trying fallback TagCategorySearch")
                    return {"message": "TagCategorySearch fallback failed"}, 502