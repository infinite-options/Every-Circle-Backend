from flask import request
from flask_restful import Resource
from datetime import datetime
import json

from sympy import group

from data_ec import connect

class BusinessServiceOptions(Resource):
    def get(self, bs_uid):
        """Get all option groups for a service, grouped by group_title."""
        print("In BusinessServiceOptions GET")
        response = {}
        try:
            with connect() as db:
                query = f"""
                    SELECT *
                    FROM every_circle.business_services_options
                    WHERE bso_business_service_id = '{bs_uid}'
                    AND bso_is_active = 1
                    ORDER BY bso_display_order ASC
                """
                result = db.execute(query)
                rows = result.get('result') or []

                # Group flat rows into nested structure matching frontend
                groups = {}
                for row in rows:
                    title = row['bso_group_title']
                    if title not in groups:
                        groups[title] = {
                            'id': title,
                            'title': title,
                            'type': row['bso_group_type'],
                            'required': bool(row['bso_required']),
                            'max_selections': row['bso_max_selections'],
                            'options': []
                        }
                    groups[title]['options'].append({
                        'id': row['bso_uid'],
                        'label': row['bso_option_label'],
                        'extra_cost': str(row['bso_extra_cost'] or '0')
                    })

                response['result'] = list(groups.values())
                response['code'] = 200
                return response, 200

        except Exception as e:
            print(f"Error in BusinessServiceOptions GET: {str(e)}")
            return {'message': 'Internal Server Error', 'code': 500}, 500

    def post(self, bs_uid):
        """
        Replace all options for a service.
        Expects JSON body: { "choice_groups": [...], "special_instructions_enabled": 0/1, "special_instructions_max_chars": 80 }
        """
        print("In BusinessServiceOptions POST")
        response = {}
        try:
            try:
                payload = request.get_json(force=True, silent=True) or {}
                if not payload:
                    raw = request.data
                    print(f"Raw request data: {raw}")
                    print(f"Content-Type: {request.content_type}")
                    if raw:
                        import json
                        payload = json.loads(raw)
                    else:
                        payload = {}
            except Exception as parse_err:
                print(f"JSON parse error: {parse_err}")
                payload = {}
            choice_groups = payload.get('choice_groups', [])
            print(f"📦 Full payload: {payload}")
            print(f"📦 choice_groups type: {type(choice_groups)}")
            print(f"📦 choice_groups length: {len(choice_groups)}")
            print(f"📦 choice_groups content: {choice_groups}")
            special_instructions_enabled = payload.get('special_instructions_enabled', 0)
            special_instructions_max_chars = payload.get('special_instructions_max_chars', 80)

            with connect() as db:
                # Verify service exists
                svc_check = db.select('every_circle.business_services', where={'bs_uid': bs_uid})
                if not svc_check['result']:
                    return {'message': 'Service not found', 'code': 404}, 404

                # Soft-delete all existing options for this service
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                db.execute(f"""
                    UPDATE every_circle.business_services_options
                    SET bso_is_active = 0, bso_updated_at = '{now}'
                    WHERE bso_business_service_id = '{bs_uid}'
                """)

                # Insert new option rows
                display_order = 1
                for group in choice_groups:
                    print(f"📦 Processing group: {group}")
                    print(f"📦 Options in group: {group.get('options', [])}")
                    group_title = group.get('title', '').strip()
                    group_type = group.get('type', 'single')
                    required = 1 if group.get('required') else 0
                    max_selections = int(group.get('max_selections') or 1)
                    options = group.get('options', [])

                    for option in options:
                        print(f"📦 Processing option: {option}")
                        label = option.get('label', '').strip()
                        print(f"📦 Label: '{label}', skipping: {not label}")
                 
                        if not label:
                            continue  # skip blank options

                        extra_cost = option.get('extra_cost', '0')
                        try:
                            extra_cost = float(extra_cost)
                        except (TypeError, ValueError):
                            extra_cost = 0.0

                        # Get new UID
                        uid_response = db.call(procedure='new_bso_uid')
                        bso_uid = uid_response['result'][0]['new_id']

                        row_payload = {
                            'bso_uid': bso_uid,
                            'bso_business_service_id': bs_uid,
                            'bso_group_title': group_title,
                            'bso_group_type': group_type,
                            'bso_required': required,
                            'bso_max_selections': max_selections,
                            'bso_option_label': label,
                            'bso_extra_cost': extra_cost,
                            'bso_display_order': display_order,
                            'bso_is_active': 1,
                            'bso_created_at': now,
                            'bso_updated_at': now,
                        }
                        db.insert('every_circle.business_services_options', row_payload)
                        display_order += 1

                # Update special instructions on the service row itself
                db.update(
                    'every_circle.business_services',
                    {'bs_uid': bs_uid},
                    {
                        'bs_special_instructions_enabled': int(special_instructions_enabled),
                        'bs_special_instructions_max_chars': int(special_instructions_max_chars),
                    }
                )

            response['message'] = 'Options saved successfully'
            response['code'] = 200
            return response, 200

        except Exception as e:
            print(f"Error in BusinessServiceOptions POST: {str(e)}")
            return {'message': 'Internal Server Error', 'code': 500}, 500

    def delete(self, bs_uid):
        """Soft-delete all options for a service (called when service is deleted)."""
        print("In BusinessServiceOptions DELETE")
        response = {}
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with connect() as db:
                db.execute(f"""
                    UPDATE every_circle.business_services_options
                    SET bso_is_active = 0, bso_updated_at = '{now}'
                    WHERE bso_business_service_id = '{bs_uid}'
                """)
            response['message'] = 'Options deleted successfully'
            response['code'] = 200
            return response, 200

        except Exception as e:
            print(f"Error in BusinessServiceOptions DELETE: {str(e)}")
            return {'message': 'Internal Server Error', 'code': 500}, 500