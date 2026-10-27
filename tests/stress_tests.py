"""
BATERÍA DE PRUEBAS DE ESTRÉS Y RESILIENCIA DEL PIPELINE FINTECH
================================================================
Especialista Responsable: VELÁZQUEZ GALLARDO, ANDREA TAIRI (Ingeniera 3)
Materia: Ingeniería de Datos (ATD-2602) - Unidad 2
TecNM - Instituto Tecnológico de Colima

Objetivo:
Validar de manera rigurosa e implacable la política 'Zero-Crash' y la atomicidad transaccional
inyectando un lote sintético de 15 anomalías extremas y datos altamente corruptos.
"""

import logging
import os
import sys
import unittest
from pathlib import Path

# Ajustar PYTHONPATH
CURRENT_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = CURRENT_DIR.parent
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

import pandas as pd
from datetime import datetime

from src.config import DB_PATH, TESTS_DIR
from src.ing3_validacion_bd import (
    TransaccionBancariaValida,
    segregar_registros_pydantic,
    probar_transaccion_rollback_segura
)

# Configurar logger de pruebas
LOG_FILE = TESTS_DIR / "stress_test_report.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("StressTests")


class TestResilienciaPipeline(unittest.TestCase):
    """
    Suite de pruebas de estrés extremo para verificar la no-interrupción del pipeline
    y el desvío obligatorio a la Zona de Cuarentena.
    """

    def setUp(self):
        """
        Genera el lote sintético de 15 registros ultra corruptos diseñados
        para intentar vulnerar validaciones semánticas, tipos de datos e integridad referencial.
        """
        self.lote_corrupto = [
            # 1. Monto monetario negativo absurdo
            {
                "id_transaccion": "CORRUPT-001",
                "fuente_datos": "ATM",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_cuenta": "Empresarial",
                "limite_credito_mxn": 200000.0,
                "estado_cuenta": "ACTIVA",
                "tipo_operacion": "RETIRO",
                "moneda_original": "MXN",
                "monto_original": -999999.99,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": -999999.99,
                "comision_aplicada": 15.0,
                "monto_total_mxn": -999984.99,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": -999999.99,
                "estatus_operacion": "EXITOSA",
                "canal_plataforma": "ATM-COL-01",
                "clabe_beneficiario": "N/A_CAJERO_FISICO",
                "email_notificacion": "no_aplica_atm@banco.com.mx",
                "concepto": "RETIRO",
                "monto_minmax_norm": 0.0,
                "segmento_monto": "Bajo"
            },
            # 2. Monto con texto/string donde va numérico
            {
                "id_transaccion": "CORRUPT-002",
                "fuente_datos": "ATM",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "RETIRO",
                "moneda_original": "MXN",
                "monto_original": "MIL PESOS",
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": "MIL PESOS",
                "comision_aplicada": 15.0,
                "monto_total_mxn": "MIL PESOS",
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 1000.0,
                "estatus_operacion": "EXITOSA",
                "canal_plataforma": "ATM-COL-01",
                "clabe_beneficiario": "N/A_CAJERO_FISICO",
                "email_notificacion": "no_aplica_atm@banco.com.mx",
                "concepto": "RETIRO",
                "monto_minmax_norm": 0.1,
                "segmento_monto": "Bajo"
            },
            # 3. Fecha imposible de calendario
            {
                "id_transaccion": "CORRUPT-003",
                "fuente_datos": "ATM",
                "fecha_operacion": "2026-99-99",
                "timestamp_iso": "2026-99-99T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "RETIRO",
                "moneda_original": "MXN",
                "monto_original": 1000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 1000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 1000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 1000.0,
                "estatus_operacion": "EXITOSA",
                "canal_plataforma": "ATM-COL-01",
                "clabe_beneficiario": "N/A_CAJERO_FISICO",
                "email_notificacion": "no_aplica_atm@banco.com.mx",
                "concepto": "RETIRO",
                "monto_minmax_norm": 0.015,
                "segmento_monto": "Bajo"
            },
            # 4. Fecha con lenguaje natural
            {
                "id_transaccion": "CORRUPT-004",
                "fuente_datos": "ATM",
                "fecha_operacion": "ayer por la tarde",
                "timestamp_iso": "ayer",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "RETIRO",
                "moneda_original": "MXN",
                "monto_original": 500.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 500.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 500.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 500.0,
                "estatus_operacion": "EXITOSA",
                "canal_plataforma": "ATM-COL-01",
                "clabe_beneficiario": "N/A_CAJERO_FISICO",
                "email_notificacion": "no_aplica_atm@banco.com.mx",
                "concepto": "RETIRO",
                "monto_minmax_norm": 0.007,
                "segmento_monto": "Bajo"
            },
            # 5. Cuenta nula (None)
            {
                "id_transaccion": "CORRUPT-005",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": None,
                "es_cuenta_huerfana": True,
                "titular": None,
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 2000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 2000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 2000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 2000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "App Android",
                "clabe_beneficiario": "012345678901234567",
                "email_notificacion": "usuario@banco.com.mx",
                "concepto": "Nómina",
                "monto_minmax_norm": 0.029,
                "segmento_monto": "Bajo"
            },
            # 6. Cuenta huérfana inexistente en Core Bancario
            {
                "id_transaccion": "CORRUPT-006",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-9999",
                "es_cuenta_huerfana": True,
                "titular": None,
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 15000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 15000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 15000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 15000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "Web Banking",
                "clabe_beneficiario": "012345678901234567",
                "email_notificacion": "usuario@banco.com.mx",
                "concepto": "Préstamo",
                "monto_minmax_norm": 0.22,
                "segmento_monto": "Medio"
            },
            # 7. CLABE trunca de 3 dígitos
            {
                "id_transaccion": "CORRUPT-007",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 3000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 3000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 3000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 3000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "API Empresarial",
                "clabe_beneficiario": "123",
                "email_notificacion": "usuario@banco.com.mx",
                "concepto": "Servicios",
                "monto_minmax_norm": 0.044,
                "segmento_monto": "Bajo"
            },
            # 8. CLABE con caracteres alfabéticos
            {
                "id_transaccion": "CORRUPT-008",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 8000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 8000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 8000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 8000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "Web Banking",
                "clabe_beneficiario": "0123456789ABCDEF12",
                "email_notificacion": "usuario@banco.com.mx",
                "concepto": "Servicios",
                "monto_minmax_norm": 0.117,
                "segmento_monto": "Medio"
            },
            # 9. Email con inyección SQL
            {
                "id_transaccion": "CORRUPT-009",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 4000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 4000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 4000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 4000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "Web Banking",
                "clabe_beneficiario": "012345678901234567",
                "email_notificacion": "' OR '1'='1",
                "concepto": "Préstamo",
                "monto_minmax_norm": 0.058,
                "segmento_monto": "Bajo"
            },
            # 10. Inyección SQL maliciosa en titular y concepto
            {
                "id_transaccion": "CORRUPT-010",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz'; DROP TABLE transacciones_limpias;--",
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 12000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 12000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 12000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 12000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "API Empresarial",
                "clabe_beneficiario": "012345678901234567",
                "email_notificacion": "correo@@sinformato",
                "concepto": "DROP TABLE cuarentena_errores;--",
                "monto_minmax_norm": 0.176,
                "segmento_monto": "Medio"
            },
            # 11. Diccionario prácticamente vacío
            {
                "id_transaccion": "CORRUPT-011"
            },
            # 12. Monto 0.0 en RETIRO monetario (imposible)
            {
                "id_transaccion": "CORRUPT-012",
                "fuente_datos": "ATM",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "RETIRO",
                "moneda_original": "MXN",
                "monto_original": 0.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 0.0,
                "comision_aplicada": 15.0,
                "monto_total_mxn": 15.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 0.0,
                "estatus_operacion": "EXITOSA",
                "canal_plataforma": "ATM-COL-01",
                "clabe_beneficiario": "N/A_CAJERO_FISICO",
                "email_notificacion": "no_aplica_atm@banco.com.mx",
                "concepto": "RETIRO",
                "monto_minmax_norm": 0.0,
                "segmento_monto": "Bajo"
            },
            # 13. Comisión negativa absurda
            {
                "id_transaccion": "CORRUPT-013",
                "fuente_datos": "ATM",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "RETIRO",
                "moneda_original": "MXN",
                "monto_original": 1000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 1000.0,
                "comision_aplicada": -50.0,
                "monto_total_mxn": 950.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 1000.0,
                "estatus_operacion": "EXITOSA",
                "canal_plataforma": "ATM-COL-01",
                "clabe_beneficiario": "N/A_CAJERO_FISICO",
                "email_notificacion": "no_aplica_atm@banco.com.mx",
                "concepto": "RETIRO",
                "monto_minmax_norm": 0.015,
                "segmento_monto": "Bajo"
            },
            # 14. Normalización Min-Max fuera de rango (> 1.0)
            {
                "id_transaccion": "CORRUPT-014",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 2000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 2000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 2000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 2000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "App Android",
                "clabe_beneficiario": "012345678901234567",
                "email_notificacion": "usuario@banco.com.mx",
                "concepto": "Nómina",
                "monto_minmax_norm": 1.85,
                "segmento_monto": "Bajo"
            },
            # 15. Segmento económico inexistente
            {
                "id_transaccion": "CORRUPT-015",
                "fuente_datos": "SPEI",
                "fecha_operacion": "2026-09-15",
                "timestamp_iso": "2026-09-15T12:00:00",
                "id_cuenta": "CTA-0001",
                "es_cuenta_huerfana": False,
                "titular": "Rosa Ruiz",
                "tipo_operacion": "TRANSFERENCIA_SPEI",
                "moneda_original": "MXN",
                "monto_original": 50000.0,
                "tipo_cambio_aplicado": 1.0,
                "monto_mxn": 50000.0,
                "comision_aplicada": 0.0,
                "monto_total_mxn": 50000.0,
                "fue_capped_iqr": False,
                "monto_original_pre_capping": 50000.0,
                "estatus_operacion": "LIQUIDADA",
                "canal_plataforma": "Web Banking",
                "clabe_beneficiario": "012345678901234567",
                "email_notificacion": "usuario@banco.com.mx",
                "concepto": "Transferencia",
                "monto_minmax_norm": 0.735,
                "segmento_monto": "VIP_DIAMANTE"
            }
        ]

    def test_01_resiliencia_zero_crash(self):
        """
        Demuestra que el procesamiento del lote de 15 datos ultra corruptos
        NO arroja ninguna excepción no controlada (Zero-Crash Policy).
        """
        logger.info("TEST 1: Inyectando 15 registros ultra corruptos en segregar_registros_pydantic()...")
        df_corrupto = pd.DataFrame(self.lote_corrupto)
        
        try:
            df_limpias, df_cuarentena, reporte = segregar_registros_pydantic(df_corrupto)
            crash_occurred = False
        except Exception as e:
            logger.error(f"Fallo no controlado: {e}")
            crash_occurred = True

        self.assertFalse(crash_occurred, "El pipeline colapsó ante datos corruptos.")
        logger.info("TEST 1 PASSED: El pipeline procesó el lote sin colapsar (Zero Crash).")

    def test_02_interceptacion_total_en_cuarentena(self):
        """
        Demuestra que el 100% de los 15 registros corruptos fue detectado, rechazado
        y canalizado a la Zona de Cuarentena con motivo explícito.
        """
        logger.info("TEST 2: Verificando aislamiento del 100% de la basura inyectada...")
        df_corrupto = pd.DataFrame(self.lote_corrupto)
        df_limpias, df_cuarentena, reporte = segregar_registros_pydantic(df_corrupto)

        self.assertEqual(len(df_limpias), 0, "Se colaron registros corruptos a producción.")
        self.assertEqual(len(df_cuarentena), 15, "No se capturó la totalidad del lote corrupto en cuarentena.")
        
        # Verificar que todos los registros en cuarentena contengan su causa y fecha
        for idx, row in df_cuarentena.iterrows():
            motivo = row["motivo_rechazo_cuarentena"]
            fecha = row["fecha_aislamiento_cuarentena"]
            self.assertTrue(bool(motivo and str(motivo).strip()), f"Fila {idx} carece de motivo de rechazo.")
            self.assertTrue(bool(fecha and str(fecha).strip()), f"Fila {idx} carece de timestamp de aislamiento.")
            logger.info(f" -> Registro {row.get('id_transaccion', 'N/A')}: Aislado por [{motivo}]")

        logger.info("TEST 2 PASSED: 15/15 registros interceptados y documentados en cuarentena con causa.")

    def test_03_atomicidad_acid_rollback(self):
        """
        Demuestra que ante un fallo forzado a nivel de base de datos relacional,
        el pipeline ejecuta conn.rollback() dejando la base de datos íntegra.
        """
        logger.info("TEST 3: Ejecutando prueba de fallo transaccional y validando conn.rollback()...")
        rollback_exitoso = probar_transaccion_rollback_segura(DB_PATH)
        self.assertTrue(rollback_exitoso, "El rollback no preservó la integridad de la base de datos.")
        logger.info("TEST 3 PASSED: Transacción revertida de forma atómica. Cero escrituras parciales.")


def correr_bateria_estres():
    print("\n" + "=" * 80)
    print(" INICIANDO BATERÍA DE PRUEBAS DE ESTRÉS Y RESILIENCIA (ING. 3 ANDREA VELÁZQUEZ)")
    print("=" * 80)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestResilienciaPipeline)
    runner = unittest.TextTestRunner(verbosity=2)
    resultado = runner.run(suite)
    print("=" * 80)
    print(f" RESULTADO FINAL DE PRUEBAS DE ESTRÉS: {'EXITOSO (100% PASS)' if resultado.wasSuccessful() else 'FALLIDO'}")
    print(f" Log detallado almacenado en: {LOG_FILE}")
    print("=" * 80 + "\n")
    return resultado.wasSuccessful()


if __name__ == "__main__":
    correr_bateria_estres()
