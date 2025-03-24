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

def pretty_to_bytes(pretty_str):
    units = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    num, unit = float(pretty_str[:-1]), pretty_str[-1].upper()
    return int(num * units.get(unit, 1))

@app.route('/ga4gh/drs/v1/objects/<drs_uuid>')
def get_drs_object(drs_uuid):
    body = {
        "id": "",
        "self_uri": "",
        "size": 0,
        "created_time": "",
        "checksums": [],
        # Required for single blob
        "access_methods": [],
        # "contents": [
        #     {
        #         "name": "string", # Required
        #         "drs_uri": "drs://drs.example.org/314159",
        #     }
        # ],
    }

    # TODO:
    # First we try to find the object in the manifest table
    query = """
    SELECT * FROM manifest WHERE uuid = %s;
    """

    object = execute_sql_query(query, (drs_uuid,))
    if len(object) > 1:
        # This should be an error, should only be one uuid in the system
        pass
    elif len(object) == 0:
        # If it doesn't then we try to find it on the files table
        query = """
        SELECT files.*, manifest.creation_date 
        FROM files         
        LEFT JOIN manifest ON manifest.hubmap_id = files.hubmap_id
        WHERE file_uuid = %s;
        """
        object = execute_sql_query(query, (drs_uuid,))
        if len(object) > 1:
            # Same error as above
            pass
        elif len(object) == 0:
            # If neither exists, 404.
            pass
        else:
            body["id"] = object[0]["uuid"]
            body["name"] = object[0]["name"]
            body["size"] = object[0]["size"]
            body["created_time"] = object[0]["creation_date"]
            body["access_methods"] = [{
                "type": "https",
                "access_url": {
                    "url": "https://fake_url_for_testing.com"
                }
            }]
            # If it exists, then we build the individual object response
            # Build the metadata object
            pass
    else:
        # If it exists, then we know we have to build the bundle
        # Build the metadata object
        body["id"] = object[0]["uuid"]
        body["size"] = pretty_to_bytes(object[0]["pretty_size"])
        body["created_time"] = object[0]["creation_date"]
        body["checksums"] = [{"checksum": "", "type": "md5"}]
        body["description"] = f"{object[0]['hubmap_id']} - {object[0]['dataset_type']} dataset"

        # TODO Need info from the files
        # query = """
        # SELECT * FROM files WHERE manifest_id = %s;
        # """
        # contents = execute_sql_query(query, (object[0]["manifest_id"],))

    return jsonify(body)

@app.route('/datasets', methods=['GET'])
def get_included_datasets():
    query = """
    SELECT DISTINCT uuid FROM manifest;
    """

    matches = execute_sql_query(query)
    return jsonify(matches)

def create_app():
    return app

if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1", port="5000")
