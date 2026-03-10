import snowflake.connector
from flask import Flask, request, jsonify
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import json
import os

app = Flask(__name__)

# --- CONFIGURACIÓN DEL LIMITADOR ---
# Esto limitará por la dirección IP de quien consulta
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per day", "50 per hour"], # Límites globales
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

# --- APLICAR LÍMITE ESPECÍFICO A ESTA RUTA ---
@app.route('/get_eta', methods=['GET'])
@limiter.limit("10 per minute")  # Solo 10 consultas por minuto por persona
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
        
        # ... (Tu query sigue igual aquí abajo) ...
        # [Se mantiene el resto del código que ya tenías]
