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
                'user_uid', 'page_name', 'feedback_text',
                'question_1', 'question_2',
                'question_3'
            ]
            
            for field in required_fields:
                if field not in data:
                    return {
                        'message': f'Missing required field: {field}',
                        'code': 400
                    }, 400
            
            # Validate ratings are between 1-5
            ratings = [
                data['question_1'],
                data['question_2'],
                data['question_3']
            ]
            
            for rating in ratings:
                if not isinstance(rating, int) or rating < 1 or rating > 5:
                    return {
                        'message': 'Ratings must be integers between 1 and 5',
                        'code': 400
                    }, 400
            
            # Call stored procedure
            query = """
                CALL every_circle.create_feedback(
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            params = [
                data['user_uid'],
                data.get('first_name', ''),
                data.get('last_name', ''),
                data['page_name'],
                data['feedback_text'],
                data['question_1'],
                data['question_2'],
                data['question_3']
            ]
            
            print('Calling stored procedure with params:', params)
            
            with connect() as db:
                response = db.execute(query, params)
            
            print('Stored procedure response:', response)
            
            return {
                'message': 'Feedback submitted successfully',
                'code': 200
            }, 200
                
        except Exception as e:
            print(f"Error submitting feedback: {str(e)}")
            return {
                'message': f'Error submitting feedback: {str(e)}',
                'code': 500
            }, 500
    
    def get(self):
        """
        Get feedback data (admin/analytics endpoint)
        Optional query params:
        - user_uid: Filter by user
        - page_name: Filter by page
        - limit: Number of results (default 100)
        """
        try:
            user_uid = request.args.get('user_uid')
            page_name = request.args.get('page_name')
            limit = request.args.get('limit', 100, type=int)
            
            query = "SELECT * FROM every_circle.feedback WHERE 1=1"
            params = []
            
            if user_uid:
                query += " AND feedback_user_uid = %s"
                params.append(user_uid)
            
            if page_name:
                query += " AND feedback_page_name = %s"
                params.append(page_name)
            
            query += " ORDER BY feedback_created_at DESC LIMIT %s"
            params.append(limit)
            
            with connect() as db:
                response = db.execute(query, params)
            
            if response and 'result' in response:
                return {
                    'feedback': response['result'],
                    'count': len(response['result']),
                    'code': 200
                }, 200
            else:
                return {
                    'message': 'No feedback found',
                    'code': 404
                }, 404
                
        except Exception as e:
            print(f"Error fetching feedback: {str(e)}")
            return {
                'message': f'Error fetching feedback: {str(e)}',
                'code': 500
            }, 500