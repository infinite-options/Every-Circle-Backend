from flask_restful import Resource
from data_ec import connect

class CategoryList(Resource):
    def get(self, uid):
        print("In Category List GET")
        response = {}
        try:
            with connect() as db:
                
                if uid == "parent_level":
                    query = '''
                                SELECT *
                                FROM every_circle.category
                                WHERE category_uid LIKE '%0000'
                                ORDER BY category_name;
                            '''
                    response = db.execute(query, cmd='get')

                elif uid == "all":
                    response = db.select('every_circle.category ORDER BY category_name')

                elif uid[:3] == "220":
                    query = f'''
                                SELECT *
                                FROM every_circle.category
                                WHERE category_parent_id = '{uid}'
                                ORDER BY category_name;
                            '''
                    response = db.execute(query, cmd='get')

                else:
                    response['message'] = 'Invalid uid provided'
                    response['code'] = 400
                    return response, 400

            if not response['result']:
                response['message'] = f"No category found"
                response['code'] = 404
                return response, 404

            return response, 200

        except Exception as e:
            print(f"Error: {str(e)}")
            response['message'] = f"Internal Server Error: {str(e)}"
            response['code'] = 500
            return response, 500