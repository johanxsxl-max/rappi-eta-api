import snowflake.connector
from flask import Flask, request, jsonify
import json
import os

app = Flask(__name__)

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

@app.route('/get_eta', methods=['GET'])
def get_eta():
    order_id = request.args.get('order_id')
    country = request.args.get('country', 'co').lower()
    if not order_id:
        return jsonify({"error": "Falta order_id"}), 400
    
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("USE WAREHOUSE OPERATIONS")
        cursor.execute("USE ROLE OPERATIONS_ROLE")
        
        mi_query = f"""
        WITH post_orders AS (
            SELECT 
                '{country.upper()}' AS country,
                post.order_id, post.store_id, post.user_id, post.source, post.event,
                post.value AS post_order_eta, post.ranges_lower, post.ranges_upper,
                post.eta_parts, post.created_at_utc,
                convert_timezone(COALESCE(post.time_zone_id, o.time_zone_id), post.created_at_utc)::timestamp_ntz AS local_created,
                DATEADD('minutes', -15, local_created) AS search_start,
                DATEADD('minutes', 15, local_created) AS search_end
            FROM fivetran.predictions.{country}_post_order_etas_audit_logs post
            JOIN {country}_core_orders_public.order_eta o ON post.order_id = o.order_id
            WHERE post.order_id = {order_id}
            QUALIFY ROW_NUMBER() OVER(PARTITION BY post.order_id ORDER BY post.created_at_utc ASC) = 1
        ),
        pre_orders_filtered AS (
            SELECT 
                pre.store_id, pre.APPLICATION_USER_ID AS user_id, pre.eta, pre.eta_raw,
                pre.created_at, pre.detail_process, pre.buffer
            FROM predictions.{country}_eta_audit_logs pre
            WHERE pre.store_id IS NOT NULL AND pre.APPLICATION_USER_ID IS NOT NULL
        ),
        joined_data AS (
            SELECT 
                post.*, pre.eta, pre.eta_raw, pre.created_at AS pre_created,
                pre.detail_process, pre.buffer,
                ROW_NUMBER() OVER(PARTITION BY post.order_id 
                                 ORDER BY ABS(DATEDIFF('second', post.local_created, pre.created_at))) AS time_rank
            FROM post_orders post
            LEFT JOIN pre_orders_filtered pre 
                ON pre.store_id = post.store_id
                AND pre.user_id = post.user_id
                AND pre.created_at BETWEEN post.search_start AND post.search_end
        )
        SELECT * FROM joined_data WHERE time_rank = 1
        """
        cursor.execute(mi_query)
        columns = [col[0].lower() for col in cursor.description]
        row = cursor.fetchone()
        
        if row:
            res_dict = {}
            for i, val in enumerate(row):
                # 1. Manejo de fechas
                if hasattr(val, 'isoformat'):
                    res_dict[columns[i]] = val.isoformat()
                # 2. LIMPIEZA DE JSONS (Aquí está el truco)
                elif isinstance(val, str) and val.strip().startswith('{'):
                    try:
                        res_dict[columns[i]] = json.loads(val)
                    except:
                        res_dict[columns[i]] = val
                else:
                    res_dict[columns[i]] = val
            
            return jsonify({"status": "success", "data": res_dict})
        return jsonify({"status": "not_found"}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
