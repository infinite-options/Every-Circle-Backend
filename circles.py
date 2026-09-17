from flask import request
from flask_restful import Resource
from datetime import datetime
from data_ec import connect
from profile_status import is_profile_deleted, tombstone_network_fields

class Circles(Resource):

    @staticmethod
    def _apply_tombstone_to_circle_row(row):
        if not row:
            return row
        if not is_profile_deleted(row):
            row["is_deleted"] = False
            return row
        uid = row.get("profile_personal_uid") or row.get("circle_related_person_id")
        row["is_deleted"] = True
        tombstone = tombstone_network_fields(uid, True)
        for key in (
            "profile_personal_user_id",
            "profile_personal_first_name",
            "profile_personal_last_name",
            "user_email_id",
            "profile_personal_email_audience",
            "profile_personal_phone_number",
            "profile_personal_phone_number_audience",
            "profile_personal_image",
            "profile_personal_image_audience",
            "profile_personal_city",
            "profile_personal_state",
            "profile_personal_country",
            "profile_personal_city_audience",
            "profile_personal_state_audience",
            "profile_personal_latitude",
            "profile_personal_longitude",
        ):
            if key in tombstone:
                row[key] = tombstone.get(key)
            elif key in row:
                row[key] = None
        row.update({k: tombstone.get(k) for k in tombstone if k.startswith("profile_personal_")})
        return row
    
    def get(self, circle_id):
        circle_profile_id = circle_id
        print(f"In Circles GET with circle_profile_id: {circle_profile_id}")
        response = {}
        
        try:
            if not circle_profile_id:
                response['message'] = 'circle_profile_id is required'
                response['code'] = 400
                return response, 400
            
            # Get optional circle_related_person_id from query parameters
            circle_related_person_id = request.args.get('circle_related_person_id')
            print(f"circle_related_person_id from query params: {circle_related_person_id}")
            
            with connect() as db:
                # Build query based on whether circle_related_person_id is provided
                if circle_related_person_id:
                    circles_query = """
                        SELECT circles.*
                            , pp.profile_personal_uid, pp.profile_personal_user_id, pp.profile_personal_first_name, pp.profile_personal_last_name
                            , u.user_email_id
                            , pp.profile_personal_email_audience
                            , pp.profile_personal_phone_number
                            , pp.profile_personal_phone_number_audience
                            , pp.profile_personal_image
                            , pp.profile_personal_image_audience
                            , pp.profile_personal_city, pp.profile_personal_state, pp.profile_personal_country
                            , pp.profile_personal_city_audience, pp.profile_personal_state_audience
                            , pp.profile_personal_latitude, pp.profile_personal_longitude
                            , pp.profile_personal_is_deleted
                        FROM every_circle.circles
                        LEFT JOIN profile_personal pp ON circle_related_person_id = profile_personal_uid
                        LEFT JOIN users u ON pp.profile_personal_user_id = user_uid
                        WHERE circle_profile_id = %s
                        AND circle_related_person_id = %s
                        ORDER BY circle_date DESC, circle_uid DESC
                    """
                    query_params = (circle_profile_id, circle_related_person_id)
                    print(f"Executing GET query for circle_profile_id: {circle_profile_id} AND circle_related_person_id: {circle_related_person_id}")
                else:
                    # Query to get all circles for a profile
                    circles_query = """
                        SELECT circles.*
                            , pp.profile_personal_uid, pp.profile_personal_user_id, pp.profile_personal_first_name, pp.profile_personal_last_name
                            , u.user_email_id
                            , pp.profile_personal_email_audience
                            , pp.profile_personal_phone_number
                            , pp.profile_personal_phone_number_audience
                            , pp.profile_personal_image
                            , pp.profile_personal_image_audience
                            , pp.profile_personal_city, pp.profile_personal_state, pp.profile_personal_country
                            , pp.profile_personal_city_audience, pp.profile_personal_state_audience
                            , pp.profile_personal_latitude, pp.profile_personal_longitude
                            , pp.profile_personal_is_deleted
                        FROM every_circle.circles
                        LEFT JOIN profile_personal pp ON circle_related_person_id = profile_personal_uid
                        LEFT JOIN users u ON pp.profile_personal_user_id = user_uid
                        -- WHERE circle_profile_id = '110-000018' AND circle_relationship != ""
                        WHERE circle_profile_id = %s AND circle_relationship != ""
                        ORDER BY circle_date DESC, circle_uid DESC
                    """
                    query_params = (circle_profile_id,)
                    print(f"Executing GET query for circle_profile_id: {circle_profile_id}")
                
                query_response = db.execute(circles_query, query_params)
                print(f"GET query response: {query_response}")
                
                if query_response.get('code') == 200:
                    response['message'] = 'Circles retrieved successfully'
                    response['code'] = 200
                    rows = [
                        self._apply_tombstone_to_circle_row(dict(row))
                        for row in (query_response.get('result') or [])
                    ]
                    response['data'] = rows
                    response['count'] = len(rows)
                    print(f"Successfully retrieved {response['count']} circles")
                else:
                    response['message'] = 'Query execution failed'
                    response['code'] = query_response.get('code', 500)
                    response['error'] = query_response.get('error', 'Unknown error')
                    print(f"GET query failed: {response['error']}")
                    return response, response['code']
                
                return response, 200
                
        except Exception as e:
            print(f"Error in Circles GET: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Circles POST - Create new circle")
        response = {}
        
        try:
            from auth import bind_actor

            payload = request.get_json()
            print(f"POST payload received: {payload}")

            circle_profile_id, actor_error = bind_actor(payload.get('circle_profile_id'))
            if actor_error:
                return actor_error, actor_error['code']
            
            required_fields = ['circle_profile_id']
            missing_fields = [field for field in required_fields if not circle_profile_id]
            
            if missing_fields:
                response['message'] = f"Missing required fields: {', '.join(missing_fields)}"
                response['code'] = 400
                print(f"POST validation failed: {response['message']}")
                return response, 400
            
            circle_data = {
                'circle_profile_id': circle_profile_id,
                'circle_related_person_id': payload.get('circle_related_person_id'),
                'circle_relationship': payload.get('circle_relationship'),
                'circle_date': payload.get('circle_date'),
                'circle_event': payload.get('circle_event'),
                'circle_note': payload.get('circle_note'),
                'circle_geotag': payload.get('circle_geotag'),
                'circle_city': payload.get('circle_city'),
                'circle_state': payload.get('circle_state'),
                'circle_introduced_by': payload.get('circle_introduced_by'),
                'circle_num_nodes': payload.get('circle_num_nodes')
            }
            
            print(f"Prepared circle_data: {circle_data}")
            
            with connect() as db:
                print("Calling stored procedure: new_circle_uid")
                circle_uid_response = db.call(procedure='new_circle_uid')
                print(f"Stored procedure response: {circle_uid_response}")
                
                if not circle_uid_response.get('result') or len(circle_uid_response['result']) == 0:
                    response['message'] = 'Failed to generate circle UID'
                    response['code'] = 500
                    print(f"POST failed: {response['message']}")
                    return response, 500
                
                new_circle_uid = circle_uid_response['result'][0]['new_id']
                circle_data['circle_uid'] = new_circle_uid
                print(f"Generated new circle_uid: {new_circle_uid}")
                
                print(f"Inserting circle data into database: {circle_data}")
                insert_response = db.insert('every_circle.circles', circle_data)
                print(f"Insert response: {insert_response}")
                
                if insert_response.get('code') != 200:
                    response['message'] = insert_response.get('message', 'Failed to insert circle')
                    response['code'] = insert_response.get('code', 500)
                    print(f"POST insert failed: {response['message']}")
                    return response, response['code']
                
                response['circle_uid'] = new_circle_uid
                response['message'] = 'Circle created successfully'
                response['code'] = 200
                response['data'] = circle_data
                print(f"POST successful: Circle created with UID {new_circle_uid}")
                return response, 200
                
        except Exception as e:
            print(f"Error in Circles POST: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500

    def delete(self, circle_id):
        circle_uid = circle_id
        print(f"In Circles DELETE - Removing circle with circle_uid: {circle_uid}")
        response = {}

        try:
            from auth import bind_actor

            if not circle_uid:
                response['message'] = 'circle_uid is required'
                response['code'] = 400
                return response, 400

            with connect() as db:
                existing = db.select('every_circle.circles', where={'circle_uid': circle_uid})
                if not existing.get('result') or len(existing['result']) == 0:
                    response['message'] = f'Circle with UID {circle_uid} not found'
                    response['code'] = 404
                    return response, 404

                _, actor_error = bind_actor(existing['result'][0].get('circle_profile_id'))
                if actor_error:
                    return actor_error, actor_error['code']

                delete_response = db.delete(f"DELETE FROM every_circle.circles WHERE circle_uid = '{circle_uid}'")
                print(f"Delete response: {delete_response}")

                if delete_response.get('code') != 200:
                    response['message'] = delete_response.get('message', 'Failed to delete circle')
                    response['code'] = delete_response.get('code', 500)
                    return response, response['code']

                response['message'] = 'Connection removed successfully'
                response['code'] = 200
                response['circle_uid'] = circle_uid
                print(f"DELETE successful: Circle {circle_uid} removed")
                return response, 200

        except Exception as e:
            print(f"Error in Circles DELETE: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500

    def put(self, circle_id):
        circle_uid = circle_id
        print(f"In Circles PUT - Update circle with circle_uid: {circle_uid}")
        response = {}
        
        try:
            if not circle_uid:
                response['message'] = 'circle_uid is required'
                response['code'] = 400
                print(f"PUT validation failed: {response['message']}")
                return response, 400
            
            # Get JSON payload from request
            payload = request.get_json()
            print(f"PUT payload received: {payload}")
            
            if not payload:
                response['message'] = 'Request body is required'
                response['code'] = 400
                print(f"PUT validation failed: {response['message']}")
                return response, 400
            
            with connect() as db:
                # Check if circle exists
                print(f"Checking if circle exists with circle_uid: {circle_uid}")
                existing_circle = db.select('every_circle.circles', where={'circle_uid': circle_uid})
                print(f"Existing circle query result: {existing_circle}")
                
                if not existing_circle.get('result') or len(existing_circle['result']) == 0:
                    response['message'] = f'Circle with UID {circle_uid} not found'
                    response['code'] = 404
                    print(f"PUT failed: {response['message']}")
                    return response, 404

                from auth import bind_actor

                _, actor_error = bind_actor(
                    existing_circle['result'][0].get('circle_profile_id')
                )
                if actor_error:
                    return actor_error, actor_error['code']
                
                # Prepare update data (only include fields that are provided)
                update_data = {}
                updatable_fields = [
                    'circle_uid',
                    'circle_related_person_id',
                    'circle_relationship',
                    'circle_date',
                    'circle_event',
                    'circle_note',
                    'circle_geotag',
                    'circle_introduced_by'
                ]
                
                for field in updatable_fields:
                    if field in payload:
                        update_data[field] = payload[field]
                
                if not update_data:
                    response['message'] = 'No fields to update'
                    response['code'] = 400
                    print(f"PUT validation failed: {response['message']}")
                    return response, 400
                
                print(f"Prepared update_data: {update_data}")
                
                # Update circle
                print(f"Updating circle with circle_uid: {circle_uid}")
                update_response = db.update('every_circle.circles', {'circle_uid': circle_uid}, update_data)
                print(f"Update response: {update_response}")
                
                if update_response.get('code') != 200:
                    response['message'] = update_response.get('message', 'Failed to update circle')
                    response['code'] = update_response.get('code', 500)
                    print(f"PUT update failed: {response['message']}")
                    return response, response['code']
                
                # Get updated circle data
                print(f"Fetching updated circle data for circle_uid: {circle_uid}")
                updated_circle = db.select('every_circle.circles', where={'circle_uid': circle_uid})
                print(f"Updated circle query result: {updated_circle}")
                
                response['circle_uid'] = circle_uid
                response['message'] = 'Circle updated successfully'
                response['code'] = 200
                response['data'] = updated_circle.get('result', [{}])[0] if updated_circle.get('result') else {}
                print(f"PUT successful: Circle {circle_uid} updated")
                return response, 200
                
        except Exception as e:
            print(f"Error in Circles PUT: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500

