from flask import Flask, jsonify, request
import pymysql.cursors

app = Flask(__name__)

# Configure MySQL
MYSQL_HOST = ''
MYSQL_USER = ''
MYSQL_PASSWORD = ''
MYSQL_DB = ''

def connect_to_database():
    connection = pymysql.connect(host=MYSQL_HOST,
                                 user=MYSQL_USER,
                                 password=MYSQL_PASSWORD,
                                 database=MYSQL_DB,
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

def execute_sql_query(query, params=None):
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
    finally:
        connection.close()
    return result

@app.route('/ga4gh/drs/v1/objects/<hubmap_id>')
def get_matches():
    query = """
    SELECT drs_uri FROM files
    INNER JOIN manifest ON manifest.hubmap_id = files.hubmap_id
    WHERE manifest.hubmap_id = %s;
    """

    matches = execute_sql_query(query, (hubmap_id,))
    return jsonify(matches)

@app.route('/datasets', methods=['GET'])
def get_included_datasets():
    query = """
    SELECT DISTINCT hubmap_id FROM manifest;
    """

    matches = execute_sql_query(query)
    return jsonify(matches)

def create_app():
    return app

if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1", port="5000")
