"""
================================================================================
 SERVIDOR WEB EN TIEMPO REAL - PIPELINE ETL FINTECH (GRUPO 2)
 Materia: Ingeniería de Datos (ATD-2602) - Prof. Armando Román Gallardo Heredia
 TecNM - Instituto Tecnológico de Colima | Semestre 2026-2
================================================================================
"""
import os
import sys
import json
import time
import socket
import sqlite3
import subprocess
import threading
import webbrowser
from pathlib import Path
from flask import Flask, render_template, jsonify, request, Response, send_from_directory

# Base paths
ROOT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = ROOT_DIR / "Pipeline" if (ROOT_DIR / "Pipeline").exists() else ROOT_DIR
DB_PATH = PIPELINE_DIR / "database" / "fintech_pipeline.db"
CACHE_JSON = PIPELINE_DIR / "database" / "audit_data_cache.json"
ASSETS_DIR = ROOT_DIR / "docs" / "assets" if (ROOT_DIR / "docs" / "assets").exists() else ROOT_DIR / "Presentación" / "assets"
CHARTS_DIR = PIPELINE_DIR / "outputs" / "charts"
TEMPLATES_DIR = ROOT_DIR / "templates"

# Add pipeline src to sys.path for live validation
sys.path.insert(0, str(PIPELINE_DIR))
try:
    from src.ing3_validacion_bd import TransaccionBancariaValida
    from pydantic import ValidationError
except Exception as e:
    TransaccionBancariaValida = None
    ValidationError = None

app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=str(ROOT_DIR / "static"))

# Helper to check DB stats
def get_db_stats():
    if not DB_PATH.exists():
        return {"limpias": 0, "cuarentena": 0, "cuentas": 0, "db_exists": False}
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM transacciones_limpias")
        limpias = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM cuarentena_errores")
        cuarentena = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM catalogo_cuentas")
        cuentas = cur.fetchone()[0]
        conn.close()
        return {
            "limpias": limpias,
            "cuarentena": cuarentena,
            "cuentas": cuentas,
            "total_evaluado": limpias + cuarentena,
            "pct_limpias": round((limpias / (limpias + cuarentena) * 100), 2) if (limpias + cuarentena) > 0 else 0,
            "pct_cuarentena": round((cuarentena / (limpias + cuarentena) * 100), 2) if (limpias + cuarentena) > 0 else 0,
            "db_exists": True
        }
    except Exception as e:
        return {"error": str(e), "db_exists": False}

# Helper to get cached data
def get_cache_data():
    if CACHE_JSON.exists():
        try:
            return json.loads(CACHE_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

@app.route("/")
def index():
    stats = get_db_stats()
    cache = get_cache_data()
    return render_template("index.html", stats=stats, cache_json=json.dumps(cache))

@app.route("/api/status")
def api_status():
    return jsonify({
        "status": "ONLINE",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "stats": get_db_stats()
    })

@app.route("/api/step/<int:step_id>")
def api_step(step_id):
    cache = get_cache_data()
    key = f"step{step_id}"
    if key in cache:
        return jsonify({"step": step_id, "data": cache[key]})
    elif step_id == 7:
        return jsonify({"step": 7, "data": cache.get("stress_samples", [])})
    return jsonify({"step": step_id, "data": None, "message": "Paso no encontrado"})

@app.route("/api/quarantine")
def api_quarantine():
    filter_type = request.args.get("filter", "all")
    search = request.args.get("q", "").strip().lower()
    
    if not DB_PATH.exists():
        return jsonify({"records": [], "total": 0})
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    query = "SELECT * FROM cuarentena_errores WHERE 1=1"
    params = []
    
    if filter_type == "orphans":
        query += " AND es_cuenta_huerfana = 1"
    elif filter_type == "clabe":
        query += " AND motivo_rechazo_cuarentena LIKE '%CLABE%'"
    elif filter_type == "email":
        query += " AND motivo_rechazo_cuarentena LIKE '%EMAIL%'"
        
    if search:
        query += " AND (id_transaccion LIKE ? OR id_cuenta LIKE ? OR motivo_rechazo_cuarentena LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s])
        
    query += " ORDER BY id_registro_cuarentena ASC"
    
    cur.execute(query, params)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    
    return jsonify({
        "records": rows,
        "total": len(rows),
        "filter": filter_type,
        "search": search
    })

@app.route("/api/run-pipeline-stream", methods=["POST", "GET"])
def run_pipeline_stream():
    """Ejecuta main_pipeline.py en vivo y transmite la salida por Server-Sent Events (SSE)."""
    def generate():
        script_path = PIPELINE_DIR / "main_pipeline.py"
        yield f"data: {json.dumps({'type': 'start', 'message': 'Iniciando ejecucion en tiempo real del Pipeline ETL...'})}\n\n"
        
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(PIPELINE_DIR)
        )
        
        step_patterns = [
            ("Fase 1", 15, "Fase 1: Deduplicación ATM completada"),
            ("Fase 2", 30, "Fase 2: Tratamiento de nulos completado"),
            ("Fase 3", 45, "Fase 3: Outliers y Winsorizing IQR completado"),
            ("Fase 4", 60, "Fase 4: Integración canónica y validación de cuentas huérfanas"),
            ("Fase 5", 75, "Fase 5: Escalamiento Min-Max y Segmentación"),
            ("Fase 6", 90, "Fase 6: Validación Pydantic y persistencia en SQLite"),
            ("Fase 7", 100, "Fase 7: Pipeline finalizado con éxito")
        ]
        
        current_percent = 5
        
        for line in iter(proc.stdout.readline, ''):
            if not line:
                break
            clean_line = line.strip()
            
            # Check for phase transitions
            for pat, pct, desc in step_patterns:
                if pat in clean_line:
                    current_percent = pct
                    yield f"data: {json.dumps({'type': 'progress', 'percent': pct, 'label': desc})}\n\n"
                    break
                    
            yield f"data: {json.dumps({'type': 'log', 'text': clean_line, 'percent': current_percent})}\n\n"
            
        proc.stdout.close()
        return_code = proc.wait()
        
        stats = get_db_stats()
        yield f"data: {json.dumps({'type': 'complete', 'code': return_code, 'stats': stats})}\n\n"
        
    return Response(generate(), mimetype="text/event-stream")

@app.route("/api/run-stress-stream", methods=["POST", "GET"])
def run_stress_stream():
    """Ejecuta stress_tests.py en vivo y transmite el reporte de pruebas."""
    def generate():
        script_path = PIPELINE_DIR / "tests" / "stress_tests.py"
        yield f"data: {json.dumps({'type': 'start', 'message': 'Ejecutando suite automatizada de pruebas de estres y resiliencia...'})}\n\n"
        
        proc = subprocess.Popen(
            [sys.executable, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(PIPELINE_DIR / "tests")
        )
        
        for line in iter(proc.stdout.readline, ''):
            if not line:
                break
            clean_line = line.strip()
            yield f"data: {json.dumps({'type': 'log', 'text': clean_line})}\n\n"
            
        proc.stdout.close()
        return_code = proc.wait()
        yield f"data: {json.dumps({'type': 'complete', 'code': return_code, 'result': '100% PASS' if return_code == 0 else 'FAIL'})}\n\n"
        
    return Response(generate(), mimetype="text/event-stream")

@app.route("/api/test-corrupt-record", methods=["POST"])
def test_corrupt_record():
    """Valida un registro con Pydantic v2 en tiempo real y devuelve el resultado."""
    data = request.json or {}
    idx = data.get("index")
    
    cache = get_cache_data()
    samples = cache.get("stress_samples", [])
    
    if idx is not None and 0 <= idx < len(samples):
        sample = samples[idx]
    else:
        sample = data.get("record", {})
        
    # Generate test record based on sample
    test_dict = {
        "id_transaccion": sample.get("id", "TEST-CORRUPT"),
        "fuente_datos": "ATM",
        "fecha_operacion": sample.get("fecha", "2026-09-15"),
        "timestamp_iso": "2026-09-15T12:00:00",
        "id_cuenta": sample.get("cuenta", "CTA-0001"),
        "es_cuenta_huerfana": (sample.get("cuenta") == "CTA-9999"),
        "titular": "Cliente Prueba",
        "tipo_cuenta": "Ahorro",
        "limite_credito_mxn": 50000.0,
        "estado_cuenta": "ACTIVA",
        "tipo_operacion": sample.get("tipo_op", "RETIRO"),
        "moneda_original": "MXN",
        "monto_original": sample.get("monto", 1000.0),
        "tipo_cambio_aplicado": 1.0,
        "monto_mxn": sample.get("monto", 1000.0),
        "comision_aplicada": sample.get("comision", 15.0),
        "monto_total_mxn": 1015.0,
        "fue_capped_iqr": False,
        "monto_original_pre_capping": 1000.0,
        "estatus_operacion": "EXITOSA",
        "canal_plataforma": "ATM-COL-01",
        "clabe_beneficiario": sample.get("clabe", "N/A_CAJERO_FISICO"),
        "email_notificacion": sample.get("email", "test@banco.com.mx"),
        "concepto": "RETIRO",
        "monto_minmax_norm": sample.get("minmax", 0.05),
        "segmento_monto": sample.get("segmento", "Bajo")
    }
    
    if TransaccionBancariaValida is None:
        return jsonify({
            "status": "CUARENTENA",
            "motivo": sample.get("motivo", "Error de validación Pydantic"),
            "action": "Aislado en tabla 'cuarentena_errores'",
            "record": test_dict
        })
        
    try:
        val = TransaccionBancariaValida(**test_dict)
        return jsonify({
            "status": "CONFORME",
            "message": "Registro válido (Cumple 100% el esquema canónico)",
            "action": "Persistido en tabla 'transacciones_limpias'",
            "record": test_dict
        })
    except ValidationError as ve:
        errors = [f"{e.get('loc')}: {e.get('msg')}" for e in ve.errors()]
        return jsonify({
            "status": "CUARENTENA",
            "motivo": "; ".join(errors),
            "errors": errors,
            "action": "Aislado en tabla 'cuarentena_errores' (Zero Crash)",
            "record": {k: str(v) for k, v in test_dict.items()}
        })
    except Exception as ex:
        return jsonify({
            "status": "CUARENTENA",
            "motivo": str(ex),
            "action": "Aislado en tabla 'cuarentena_errores'",
            "record": test_dict
        })

@app.route("/assets/<path:filename>")
def serve_assets(filename):
    return send_from_directory(str(ASSETS_DIR), filename)

@app.route("/charts/<path:filename>")
def serve_charts(filename):
    return send_from_directory(str(CHARTS_DIR), filename)

def find_free_port(start_port=5000):
    for port in range(start_port, start_port + 20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('127.0.0.1', port)) != 0:
                return port
    return start_port

def open_browser(url):
    time.sleep(1.2)
    print(f"\n=======================================================================")
    print(f" [OK] Servidor iniciado con exito!")
    print(f" Abriendo navegador en: {url}")
    print(f" Presione CTRL+C en esta ventana para detener el servidor.")
    print(f"=======================================================================\n")
    try:
        webbrowser.open_new(url)
    except Exception as e:
        print(f"No se pudo abrir el navegador automáticamente: {e}")

if __name__ == "__main__":
    port = find_free_port(5000)
    url = f"http://127.0.0.1:{port}"
    threading.Thread(target=open_browser, args=(url,), daemon=True).start()
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
