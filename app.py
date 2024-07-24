from flask import Flask, jsonify, request
import pymysql.cursors

app = Flask(__name__)

# Configure MySQL
MYSQL_HOST = 'localhost'
MYSQL_USER = 'readonly'
MYSQL_PASSWORD = ''
MYSQL_DB = 'drs'

def connect_to_database():
    connection = pymysql.connect(host=MYSQL_HOST,
                                 user=MYSQL_USER,
                                 password=MYSQL_PASSWORD,
                                 database=MYSQL_DB,
                                 cursorclass=pymysql.cursors.DictCursor,
                                 unix_socket="/var/lib/mysql/mysql.sock")
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
    hubmap_id = request.args.get('hubmap_id')
    if not hubmap_id:
        return jsonify({'error': 'HuBMAP ID is required to perform this operation'}), 400

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
    SELECT DISTINCT hubmap_uuid FROM manifest;
    """

    matches = execute_sql_query(query)
    return jsonify(matches)

if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1")
