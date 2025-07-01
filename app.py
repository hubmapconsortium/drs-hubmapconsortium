from flask import Flask, jsonify, make_response
import pymysql.cursors
import json

app = Flask(__name__)
# Load configuration from JSON file
with open("config.json") as config_file:
    config_data = json.load(config_file)

# Configure MySQL
MYSQL_HOST = config_data["MYSQL_HOST"]
MYSQL_USER = config_data["MYSQL_USER"]
MYSQL_PASSWORD = config_data["MYSQL_PASSWORD"]
MYSQL_DB = config_data["MYSQL_DB"]
DOMAIN = config_data["DOMAIN"]
ACCESS_DOMAIN = config_data["ACCESS_DOMAIN"]


def connect_to_database():
    connection = pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        cursorclass=pymysql.cursors.DictCursor,
    )
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


@app.route("/ga4gh/drs/v1/objects/<drs_uuid>")
def get_drs_object(drs_uuid):
    body = {
        "id": "",
        "self_uri": "",
        "size": 0,
        "created_time": "",
        "checksums": [],
        "access_methods": [],
    }

    # First we try to find the object in the manifest table
    query = """
    SELECT * FROM manifest WHERE hubmap_id = %s;
    """

    object = execute_sql_query(query, (drs_uuid,))
    if len(object) > 1:
        # This should be an error, should only be one uuid in the system
        return make_response(jsonify({"msg": "More than one object found."}), 404)
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
            return make_response(jsonify({"msg": "More than one object found."}), 404)
        elif len(object) == 0:
            # If neither exists, 404.
            return make_response(jsonify({"msg": "No object found."}), 404)
        else:
            path = object[0]["name"]

            body["id"] = object[0]["file_uuid"]
            body["self_uri"] = f"drs://{DOMAIN}/{body['id']}"
            body["name"] = path.split("/")[-1]
            body["size"] = int(object[0]["size"])
            body["created_time"] = object[0]["creation_date"]

            access_path = "/" + "/".join(path.split("/")[2:])
            body["access_methods"] = [
                {
                    "type": "https",
                    "access_url": {"url": f"https://{ACCESS_DOMAIN}{access_path}"},
                    "access_id": "https"
                }
            ]
            body["checksums"] = [{"checksum": object[0]["checksum"], "type": "md5"}]
            body["description"] = f"{object[0]['hubmap_id']} - file {object[0]['name']}"
            # If it exists, then we build the individual object response
            # Build the metadata object
            pass
    else:
        # If it exists, then we know we have to build the bundle
        # Build the metadata object
        body["id"] = object[0]["hubmap_id"]
        body["self_uri"] = f"drs://{DOMAIN}/{body['id']}"
        body["size"] = pretty_to_bytes(object[0]["pretty_size"])
        body["created_time"] = object[0]["creation_date"]
        body["checksums"] = [{"checksum": "", "type": "md5"}]
        body["description"] = (
            f"{object[0]['hubmap_id']} - {object[0]['dataset_type']} dataset"
        )

        # Get data to fill out contents index
        query = """
        SELECT * FROM files WHERE hubmap_id = %s;
        """
        contents = execute_sql_query(query, (object[0]["hubmap_id"],))
        body["contents"] = [
            {
                "name": content["name"].split("/")[-1],
                "id": content["file_uuid"],
                "drs_uri": f"drs://{DOMAIN}/{content['file_uuid']}",
            }
            for content in contents
        ]

    return jsonify(body)


@app.route("/ga4gh/drs/v1/objects/<drs_uuid>/access/<access_method>")
def get_drs_object_by_access_method(drs_uuid, access_method):
    query = """
    SELECT files.*, manifest.creation_date 
    FROM files         
    LEFT JOIN manifest ON manifest.hubmap_id = files.hubmap_id
    WHERE file_uuid = %s;
    """
    object = execute_sql_query(query, (drs_uuid,))
    if len(object) > 1:
        # Same error as above
        return make_response(jsonify({"msg": "More than one object found."}), 404)
    elif len(object) == 0:
        # If neither exists, 404.
        return make_response(jsonify({"msg": "No object found."}), 404)
    else:
        path = object[0]["name"]
        access_path = "/" + "/".join(path.split("/")[2:])
        body = {
            "url": f"https://{ACCESS_DOMAIN}{access_path}"
        }

    return jsonify(body)


@app.route("/datasets", methods=["GET"])
def get_included_datasets():
    query = """
    SELECT DISTINCT hubmap_id FROM manifest;
    """

    matches = execute_sql_query(query)
    return jsonify(matches)


def create_app():
    return app


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port="5000")
