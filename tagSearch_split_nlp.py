from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import spacy
import json
from data_ec import connect

class TagSplitNLPSearch(Resource):
    
    def get(self, query):

        print('TagSplitNLPSearch query', query)

        # Load SpaCy English pipeline
        nlp = spacy.load("en_core_web_sm")
        print("here 1")

        def clean_query_spacy(query):
            doc = nlp(query.lower())
            print("here 2")
            # Keep only alphabetic tokens, lemmatize, remove stopwords
            clean_tokens = [
                token.lemma_ for token in doc
                if token.is_alpha and not token.is_stop and token.pos_ in {"NOUN", "ADJ"}
            ]
            print("here 3")
            return clean_tokens

        # word_list = query.lower().split(' ')
        word_list = clean_query_spacy(query)
        print('Cleaned word list:', word_list)

        print('word_list', word_list)
        
        like_clauses = " OR ".join([f"lower(t.tag_name) LIKE '%{word}%'" for word in word_list])
        # params = [f"%{word}%" for word in word_list]

        print('like_clauses', like_clauses)
        
        # tag_query = f"""
        #                 SELECT DISTINCT business_uid, business_name
        #                 FROM every_circle.business b
        #                 LEFT JOIN every_circle.business_tags bt
        #                 ON b.business_uid = bt.bt_business_id
        #                 LEFT JOIN every_circle.tags t
        #                 ON bt.bt_tag_id = t.tag_uid
        #                 -- WHERE lower(t.tag_name) LIKE lower('%chinese food%')
        #                 WHERE lower(t.tag_name) LIKE lower('%{query}%');
        #                 """
        tag_query = f"""
                    SELECT result.business_uid, result.business_name
                    FROM 
                    (SELECT 
                        b.business_uid as business_uid,
                        b.business_name as business_name,
                        COUNT(DISTINCT t.tag_uid) AS match_count
                    FROM every_circle.business b
                    LEFT JOIN every_circle.business_tags bt ON b.business_uid = bt.bt_business_id
                    LEFT JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
                    WHERE {like_clauses}
                    GROUP BY b.business_uid, b.business_name
                    ORDER BY match_count DESC) result;
                    """
        
        print('tag_query:', tag_query)
        try:
            with connect() as db:
                response = db.execute(tag_query, cmd='get')

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
        
        # return store, 200

