from flask import request, jsonify
from flask_restful import Resource
from data_ec import connect

class Feedback(Resource):
    def post(self):
        """
        Submit user feedback
        
        Expected JSON payload:
        {
            "user_uid": "110-000001",
            "first_name": "John",
            "last_name": "Doe",
            "page_name": "Network",
            "feedback_text": "Great feature!",
            "question_1": 5,
            "question_2": 4,
            "question_3": 5
        }
        """
        try:
            data = request.get_json()
            print('Feedback submission data:', data)
            
            # Validate required fields
            required_fields = [
                'user_uid', 
                'page_name',
                'first_name',
                'last_name',
                'feedback_text',
                'question_1', 
                'question_2',
                'question_3'
            ]
            
            for field in required_fields:
                if field not in data:
                    return {
                        'message': f'Missing required field: {field}',
                        'code': 400
                    }, 400
            
            
            with connect() as db:
                # 1️⃣ GET NEW FEEDBACK UID
                uid_result = db.execute("CALL every_circle.new_feedback_uid()")
                print("UID RESULT:", uid_result)

                if not uid_result or 'result' not in uid_result or not uid_result['result']:
                    return {
                        'message': 'Failed to generate feedback UID',
                        'code': 500
                    }, 500

                new_feedback_uid = uid_result["result"][0]["new_id"]
                print("New feedback UID:", new_feedback_uid)

                # 2️⃣ INSERT THE FEEDBACK ROW
                insert_query = """
                    INSERT INTO every_circle.feedback (
                        feedback_uid, 
                        feedback_user_uid,
                        feedback_first_name,
                        feedback_last_name,
                        feedback_page_name,
                        feedback_text,
                        feedback_question_1,
                        feedback_question_2,
                        feedback_question_3
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                insert_params = [
                    new_feedback_uid,
                    data['user_uid'],
                    data.get('first_name', ''),
                    data.get('last_name', ''),
                    data['page_name'],
                    data['feedback_text'],
                    data['question_1'],
                    data['question_2'],
                    data['question_3']
                ]
                
                print('Inserting feedback with query:', insert_query)
                print('Insert params:', insert_params)
                
                db.execute(insert_query, insert_params, 'post')

                return {
                    'message': 'Feedback submitted successfully',
                    'code': 200
                }, 200
          
        except Exception as e:
            print(f"Error submitting feedback: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'message': f'Error submitting feedback: {str(e)}',
                'code': 500
            }, 500
    
