from flask import Flask, request, jsonify
import mysql.connector

app = Flask(__name__)


def get_db_connection():
    return mysql.connector.connect(
        host='your-db-host',
        user='your-db-user',
        password='your-db-password',
        database='every_circle'
    )


@app.route('/search', methods=['GET', 'POST'])
def search_businesses():
    data = request.get_json() if request.method == 'POST' else request.args
    location = data.get('location')
    service = data.get('service')

    if not location or not service:
        return jsonify({'error': 'Missing location or service parameter'}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.callproc('search_logic_new', [location, service])

        # Fetch the output
        for result in cursor.stored_results():
            output = result.fetchall()

        conn.close()

        return jsonify({'results': output[0][0] if output else []})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
