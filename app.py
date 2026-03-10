import snowflake.connector
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import json
import os

app = Flask(__name__)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://",
)

def get_snowflake_conn():
    return snowflake.connector.connect(
        user="johan.sanchez@rappi.com",
        password=os.environ.get('SNOW_TOKEN'), 
        account="hg51401",
        warehouse='OPERATIONS',
        database='FIVETRAN',
        schema='OPS_PL_PDT',
        role='OPERATIONS_ROLE'
    )

def parse_json_fields(row, columns):
    """Función auxiliar para limpiar fechas y JSONs"""
    res_dict = {}
    for i, val in enumerate(row):
        if hasattr(val, 'isoformat'):
            res_dict[columns[i]] = val.isoformat()
        elif isinstance(val, str) and (val.strip().startswith('{') or val.strip().startswith('[')):
            try:
                res_dict[columns[i]] = json.loads(val)
            except:
                res_dict[columns[i]] = val
        else:
            res_dict[columns[i]] = val
    return res_dict

# --- ENDPOINT 1: CÁLCULO INICIAL (EL QUE YA TENÍAS) ---
@app.route('/get_eta', methods=['GET'])
@limiter.limit("10 per minute")
def get_eta():
    order_id = request.args.get('order_id')
    country = request.args.get('country', 'co').lower()
    if not order_id: return jsonify({"error": "Falta order_id"}), 400
    
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE OPERATIONS")
        cursor.execute("USE ROLE OPERATIONS_ROLE")
        
        query = f"""
        WITH post_orders AS (
            SELECT '{country.upper()}' AS country, post.order_id, post.store_id, post.user_id, post.source, post.event,
                post.value AS post_order_eta, post.ranges_lower, post.ranges_upper, post.eta_parts, post.created_at_utc,
                convert_timezone(COALESCE(post.time_zone_id, o.time_zone_id), post.created_at_utc)::timestamp_ntz AS local_created,
                DATEADD('minutes', -15, local_created) AS search_start, DATEADD('minutes', 15, local_created) AS search_end
            FROM fivetran.predictions.{country}_post_order_etas_audit_logs post
            JOIN {country}_core_orders_public.order_eta o ON post.order_id = o.order_id
            WHERE post.order_id = {order_id}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY post.order_id ORDER BY post.created_at_utc ASC) = 1
        ),
        pre_orders_filtered AS (
            SELECT pre.store_id, pre.APPLICATION_USER_ID AS user_id, pre.eta, pre.eta_raw,
                pre.created_at, pre.detail_process, pre.buffer
            FROM predictions.{country}_eta_audit_logs pre
            WHERE pre.store_id IS NOT NULL AND pre.APPLICATION_USER_ID IS NOT NULL
        )
        SELECT post.*, pre.eta, pre.eta_raw, pre.created_at AS pre_created, pre.detail_process, pre.buffer
        FROM post_orders post
        LEFT JOIN pre_orders_filtered pre ON pre.store_id = post.store_id AND pre.user_id = post.user_id
            AND pre.created_at BETWEEN post.search_start AND post.search_end
        QUALIFY ROW_NUMBER() OVER(PARTITION BY post.order_id ORDER BY ABS(DATEDIFF('second', post.local_created, pre.created_at))) = 1
        """
        cursor.execute(query)
        columns = [col[0].lower() for col in cursor.description]
        row = cursor.fetchone()
        if row: return jsonify({"status": "success", "data": parse_json_fields(row, columns)})
        return jsonify({"status": "not_found"}), 404
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'conn' in locals(): conn.close()

# --- ENDPOINT 2: ACTUALIZACIONES DE ETA (EL NUEVO) ---
@app.route('/get_updates', methods=['GET'])
@limiter.limit("10 per minute")
def get_updates():
    order_id = request.args.get('order_id')
    country = request.args.get('country', 'co').lower()
    if not order_id: return jsonify({"error": "Falta order_id"}), 400

    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE OPERATIONS")
        cursor.execute("USE ROLE OPERATIONS_ROLE")
        
        query = f"""
        SELECT 
            '{country.upper()}' AS country,
            post.order_id, post.store_id, post.user_id, post.source, post.event,
            post.value AS post_order_eta, post.ranges_lower, post.ranges_upper,
            post.eta_parts,
            convert_timezone(COALESCE(post.time_zone_id, o.time_zone_id), post.created_at_utc)::timestamp_ntz AS local_created
        FROM fivetran.predictions.{country}_post_order_etas_audit_logs post
        JOIN {country}_core_orders_public.order_eta o ON post.order_id = o.order_id
        WHERE post.order_id = {order_id}
          AND post.eta_parts:count_down IS NOT NULL
        ORDER BY post.created_at_utc ASC
        """
        cursor.execute(query)
        columns = [col[0].lower() for col in cursor.description]
        rows = cursor.fetchall()
        
        if rows:
            results = [parse_json_fields(r, columns) for r in rows]
            return jsonify({"status": "success", "count": len(results), "data": results})
        return jsonify({"status": "not_found"}), 404
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
