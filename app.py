import snowflake.connector
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import json
import os

app = Flask(__name__)

# Configuración del limitador (10 llamadas por minuto por IP)
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
    """Limpia fechas y convierte strings de JSON en objetos reales"""
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

# --- ENDPOINT 1: CÁLCULO INICIAL (CON TURBO OPTIMIZADO) ---
@app.route('/get_eta', methods=['GET'])
@limiter.limit("10 per minute")
def get_eta():
    order_id = request.args.get('order_id')
    country = request.args.get('country', 'co').upper()
    if not order_id: return jsonify({"error": "Falta order_id"}), 400
    
    tz_fallbacks = {
        'MX': 'America/Mexico_City', 'CO': 'America/Bogota', 'BR': 'America/Sao_Paulo',
        'AR': 'America/Argentina/Buenos_Aires', 'PE': 'America/Lima', 'CL': 'America/Santiago',
        'EC': 'America/Guayaquil', 'CR': 'America/Costa_Rica', 'UY': 'America/Montevideo'
    }
    tz_default = tz_fallbacks.get(country, 'UTC')

    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE OPERATIONS")
        cursor.execute("USE ROLE OPERATIONS_ROLE")
        
        query = f"""
        WITH post_orders AS (
            SELECT 
                '{country}' AS country,
                post.order_id, post.store_id, post.user_id, post.source, post.event,
                post.value AS post_order_eta, post.ranges_lower, post.ranges_upper,
                post.eta_parts, post.created_at_utc,
                convert_timezone(COALESCE(post.time_zone_id, '{tz_default}'), post.created_at_utc)::timestamp_ntz AS local_created,
                DATEADD('minutes', -5, local_created) AS search_start,
                DATEADD('minutes', 5, local_created) AS search_end,
                o.delivery_option
            FROM fivetran.predictions.{country.lower()}_post_order_etas_audit_logs post
            JOIN fivetran.{country.lower()}_core_orders_public.delivery_order o ON post.order_id = o.order_id
            WHERE post.order_id = {order_id}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY post.order_id ORDER BY post.created_at_utc ASC) = 1
        ),
        pre_orders_filtered AS (
            SELECT 
                pre.store_id, pre.APPLICATION_USER_ID AS user_id, pre.eta, pre.eta_raw,
                pre.created_at, pre.detail_process, pre.buffer, pre.travel_time
            FROM predictions.{country.lower()}_eta_audit_logs pre
            INNER JOIN post_orders p ON pre.store_id = p.store_id AND pre.APPLICATION_USER_ID = p.user_id
            WHERE pre.created_at BETWEEN p.search_start AND p.search_end
              AND pre.detail_process:trigger_name::text = 'checkout'
              AND pre.APPLICATION_USER_ID > 0
        )
        SELECT 
            p.country, p.local_created, p.order_id, p.store_id, p.user_id, p.delivery_option, 
            p.source, p.event, p.post_order_eta, pre.eta, pre.eta_raw, pre.travel_time, 
            pre.created_at AS pre_created, pre.detail_process, pre.buffer, 
            p.ranges_lower, p.ranges_upper, p.eta_parts
        FROM post_orders p
        LEFT JOIN pre_orders_filtered pre ON 1=1
        QUALIFY ROW_NUMBER() OVER(PARTITION BY p.order_id ORDER BY ABS(DATEDIFF('second', p.local_created, pre.created_at))) = 1
        """
        cursor.execute(query)
        columns = [col[0].lower() for col in cursor.description]
        row = cursor.fetchone()
        if row: return jsonify({"status": "success", "data": parse_json_fields(row, columns)})
        return jsonify({"status": "not_found"}), 404
    except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'conn' in locals(): conn.close()

# --- ENDPOINT 2: ACTUALIZACIONES DE ETA (HISTORIAL) ---
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
            convert_timezone(COALESCE(post.time_zone_id, 'UTC'), post.created_at_utc)::timestamp_ntz AS local_created,
            CASE 
                WHEN ROW_NUMBER() OVER(PARTITION BY post.order_id ORDER BY post.created_at_utc ASC) = 1 THEN TRUE 
                WHEN post.eta_parts:count_down = TRUE THEN TRUE 
                ELSE post.user_eta_updated
            END AS user_eta_update, 
            eta_publish
        FROM fivetran.predictions.{country}_post_order_etas_audit_logs post
        JOIN fivetran.{country}_core_orders_public.order_eta o ON post.order_id = o.order_id
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

# --- ENDPOINT 3: ASSIGN MONITORING (NUEVO) ---
@app.route('/get_assignments', methods=['GET'])
@limiter.limit("10 per minute")
def get_assignments():
    order_id = request.args.get('order_id')
    country = request.args.get('country', 'co').upper()
    if not order_id: return jsonify({"error": "Falta order_id"}), 400

    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE OPERATIONS")
        
        query = f"""
        SELECT 
            iteration_id AS id, '{country}' AS country, order_id, 
            data AS data_json, created_at 
        FROM FIVETRAN.{country}_PG_MS_ASSIGN_MONITORING_PUBLIC.ORDERS 
        WHERE order_id = {order_id}
          AND iteration_id IS NOT NULL 
        ORDER BY created_at DESC
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
