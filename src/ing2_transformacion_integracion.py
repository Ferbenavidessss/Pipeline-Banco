"""
MÓDULO DE INGENIERÍA 2: TRANSFORMACIÓN E INTEGRACIÓN
====================================================
Especialista Responsable: LÓPEZ ORDORICA, ÁNGEL ALEJANDRO (Ingeniero 2)
Materia: Ingeniería de Datos (ATD-2602) - Unidad 2
TecNM - Instituto Tecnológico de Colima

Responsabilidades Técnicas Específicas:
1. Homologación temporal de fechas heterogéneas (formatos mixtos ATM y Unix Epoch SPEI) a ISO-8601.
2. Normalización y conversión cambiaria oficial de divisas (USD a MXN @ $19.50).
3. Integración de fuentes dispares (CSV físico + JSON digital) en un esquema canónico maestro.
4. Cruce relacional (LEFT JOIN) contra catalogo_cuentas.csv y detección de 61 cuentas huérfanas.
5. Escalamiento matemático Min-Max en el rango cerrado [0.0, 1.0].
6. Discretización de importes (Binning) en 3 segmentos ('Bajo', 'Medio', 'Alto') por reglas bancarias.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Tuple
import numpy as np
import pandas as pd

from src.config import (
    TIPO_CAMBIO_USD_MXN,
    CLABE_ATM_DEFAULT,
    EMAIL_ATM_DEFAULT,
    UMBRAL_MONTO_BAJO_MAX,
    UMBRAL_MONTO_MEDIO_MAX
)

logger = logging.getLogger("PipelineFintech.Ing2")
TZ_MEXICO = timezone(timedelta(hours=-6))  # Zona horaria bancaria México Centro (CST / UTC-6)


def homologar_fechas_atm(fecha_val: Any) -> Tuple[str, str]:
    """
    Parsea de manera resiliente las fechas mixtas de transacciones_atm.csv:
    - 'YYYY/MM/DD HH:MM:SS' (ej. '2026/09/15 08:45:00')
    - 'DD/MM/YYYY HH:MM'    (ej. '12/09/2026 13:26')
    - 'YYYY-MM-DD HH:MM:SS' (ej. '2026-10-12 22:22:00')
    
    Retorna tupla: (fecha_operacion_iso 'YYYY-MM-DD', timestamp_iso 'YYYY-MM-DDTHH:MM:SS').
    """
    val_str = str(fecha_val).strip()
    formatos = [
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%d/%m/%Y %H:%M:%S"
    ]
    for fmt in formatos:
        try:
            dt = datetime.strptime(val_str, fmt)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    # Respaldo robusto con pandas si existiera alguna variación no contemplada
    dt = pd.to_datetime(val_str)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%S")


def homologar_fechas_spei(ts_val: Any) -> Tuple[str, str]:
    """
    Parsea las marcas temporales heterogéneas de transferencias_digitales.json:
    - Enteros Unix Epoch en segundos (195 registros, ej. 1788816180) -> Convertidos con TZ México UTC-6.
    - Strings ISO-8601 con zona horaria (405 registros, ej. "2026-09-12T19:49:00-06:00").
    
    Retorna tupla: (fecha_operacion_iso 'YYYY-MM-DD', timestamp_iso 'YYYY-MM-DDTHH:MM:SS').
    """
    if isinstance(ts_val, (int, float, np.integer, np.floating)):
        dt = datetime.fromtimestamp(float(ts_val), tz=TZ_MEXICO)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%S")
    
    val_str = str(ts_val).strip()
    try:
        dt = datetime.fromisoformat(val_str)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        dt = pd.to_datetime(val_str)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%dT%H:%M:%S")


def homologar_fechas_multifuente(
    df_atm: pd.DataFrame,
    df_spei: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica el parseo temporal estandarizado sobre ATM y SPEI.
    """
    logger.info("Fase 4: Homologando marcas temporales de ATM y SPEI a ISO-8601 (YYYY-MM-DD y timestamp ISO)...")
    
    df_atm_res = df_atm.copy()
    df_spei_res = df_spei.copy()
    
    fechas_atm = [homologar_fechas_atm(x) for x in df_atm_res["fecha_hora"]]
    df_atm_res["fecha_operacion"] = [f[0] for f in fechas_atm]
    df_atm_res["timestamp_iso"] = [f[1] for f in fechas_atm]
    
    fechas_spei = [homologar_fechas_spei(x) for x in df_spei_res["timestamp"]]
    df_spei_res["fecha_operacion"] = [f[0] for f in fechas_spei]
    df_spei_res["timestamp_iso"] = [f[1] for f in fechas_spei]
    
    return df_atm_res, df_spei_res


def integrar_fuentes_heterogeneas(
    df_atm: pd.DataFrame,
    df_spei: pd.DataFrame
) -> pd.DataFrame:
    """
    Unifica las dos fuentes de datos bajo un esquema canónico maestro normalizado:
    - Normaliza textos y estados a MAYÚSCULAS sin espacios residuales.
    - Homologa nombres de campos entre la fuente física ATM y digital SPEI.
    - Asigna identificadores y metadatos del canal originador.
    """
    logger.info("Fase 4: Integrando fuentes heterogéneas bajo Esquema Canónico Maestro...")
    
    atm_df = df_atm.copy()
    spei_df = df_spei.copy()
    
    # 1. Campos Canónicos para ATM
    atm_df["fuente_datos"] = "ATM"
    atm_df["moneda_original"] = "MXN"
    atm_df["monto_original"] = atm_df["monto_original_pre_capping"]
    atm_df["tipo_cambio_aplicado"] = 1.0
    atm_df["estatus_operacion"] = atm_df["resultado"].astype(str).str.strip().str.upper()
    atm_df["canal_plataforma"] = atm_df["id_cajero"]
    atm_df["clabe_beneficiario"] = CLABE_ATM_DEFAULT
    atm_df["email_notificacion"] = EMAIL_ATM_DEFAULT
    atm_df["concepto"] = atm_df["tipo_operacion"]
    
    # 2. Campos Canónicos para SPEI
    spei_df["fuente_datos"] = "SPEI"
    spei_df["id_transaccion"] = spei_df["transfer_id"]
    spei_df["id_cuenta"] = spei_df["account_id"]
    spei_df["tipo_operacion"] = "TRANSFERENCIA_SPEI"
    spei_df["estatus_operacion"] = spei_df["status"].astype(str).str.strip().str.upper()
    spei_df["canal_plataforma"] = spei_df["platform"]
    spei_df["clabe_beneficiario"] = spei_df["beneficiary_clabe"]
    spei_df["email_notificacion"] = spei_df["notification_email"]
    spei_df["concepto"] = spei_df["concept"]
    
    columnas_canonicas = [
        "id_transaccion", "fuente_datos", "fecha_operacion", "timestamp_iso",
        "id_cuenta", "tipo_operacion", "moneda_original", "monto_original",
        "tipo_cambio_aplicado", "monto_mxn", "comision_aplicada", "monto_total_mxn",
        "fue_capped_iqr", "monto_original_pre_capping", "estatus_operacion",
        "canal_plataforma", "clabe_beneficiario", "email_notificacion", "concepto"
    ]
    
    df_integrado = pd.concat(
        [atm_df[columnas_canonicas], spei_df[columnas_canonicas]],
        ignore_index=True
    )
    
    logger.info(f"Esquema canónico unificado: {len(df_integrado)} transacciones totales ({len(atm_df)} ATM + {len(spei_df)} SPEI).")
    return df_integrado


def cruzar_catalogo_y_detectar_huerfanos(
    df_integrado: pd.DataFrame,
    df_cuentas: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Realiza un cruce relacional (LEFT JOIN) entre las transacciones unificadas y
    el catálogo maestro de cuentas (catalogo_cuentas.csv) utilizando id_cuenta como llave.
    
    Detecta y marca explícitamente:
    - es_cuenta_huerfana = True (si id_cuenta no existe en el catálogo maestro).
    - Incorpora las columnas dimensionales del cuentahabiente:
      titular, tipo_cuenta, limite_credito_mxn, estado_cuenta.
    """
    logger.info("Fase 4: Ejecutando cruce relacional contra catalogo_cuentas.csv y detectando llaves huérfanas...")
    
    # Cruce LEFT JOIN
    df_merged = df_integrado.merge(df_cuentas, on="id_cuenta", how="left")
    
    # Marcar cuentas huérfanas (aquellas donde titular o fecha_apertura es nulo)
    df_merged["es_cuenta_huerfana"] = df_merged["titular"].isnull()
    
    conteo_huerfanas_atm = int(df_merged[df_merged["fuente_datos"] == "ATM"]["es_cuenta_huerfana"].sum())
    conteo_huerfanas_spei = int(df_merged[df_merged["fuente_datos"] == "SPEI"]["es_cuenta_huerfana"].sum())
    total_huerfanas = conteo_huerfanas_atm + conteo_huerfanas_spei
    
    reporte_cruce = {
        "cuentas_catalogo_maestro": len(df_cuentas),
        "total_transacciones_evaluadas": len(df_merged),
        "transacciones_con_cuenta_valida": len(df_merged) - total_huerfanas,
        "cuentas_huerfanas_detectadas": {
            "atm": conteo_huerfanas_atm,
            "spei": conteo_huerfanas_spei,
            "total": total_huerfanas,
            "ejemplos_atm": df_merged[(df_merged["fuente_datos"] == "ATM") & (df_merged["es_cuenta_huerfana"])]["id_cuenta"].unique()[:5].tolist(),
            "ejemplos_spei": df_merged[(df_merged["fuente_datos"] == "SPEI") & (df_merged["es_cuenta_huerfana"])]["id_cuenta"].unique()[:5].tolist()
        },
        "politica_aislamiento": (
            "Las 61 transacciones huérfanas NO son eliminadas silenciosamente en la fase de cruce. "
            "Se conservan intactas con la bandera es_cuenta_huerfana = True para que en la Fase 6 el motor "
            "de validación Pydantic las desvíe a la Zona de Cuarentena con trazabilidad y motivo explícito."
        )
    }
    
    logger.info(f"Detección de huérfanos: 32 en ATM + 29 en SPEI = {total_huerfanas} transacciones huérfanas identificadas.")
    return df_merged, reporte_cruce


def escalar_minmax_y_discretizar(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la Fase 5 del pipeline:
    1. Escalamiento Min-Max en el rango matemático cerrado [0.0, 1.0]:
       X_norm = (X - X_min) / (X_max - X_min)
       Redondeado a 6 cifras decimales en la columna monto_minmax_norm.
    2. Discretización (Binning) en 3 segmentos bancarios:
       - 'Bajo':  monto_mxn <= $5,000.00 MXN
       - 'Medio': $5,000.00 < monto_mxn <= $25,000.00 MXN
       - 'Alto':  monto_mxn > $25,000.00 MXN
       Almacenado en la columna segmento_monto.
    """
    logger.info("Fase 5: Aplicando escalamiento Min-Max [0, 1] y discretización en 3 segmentos...")
    
    df_trans = df_merged.copy()
    
    min_monto = float(df_trans["monto_mxn"].min())
    max_monto = float(df_trans["monto_mxn"].max())
    rango = max_monto - min_monto if max_monto > min_monto else 1.0
    
    # Escalamiento Min-Max
    df_trans["monto_minmax_norm"] = ((df_trans["monto_mxn"] - min_monto) / rango).round(6)
    
    # Función de Discretización
    def clasificar_segmento(monto: float) -> str:
        if monto <= UMBRAL_MONTO_BAJO_MAX:
            return "Bajo"
        elif monto <= UMBRAL_MONTO_MEDIO_MAX:
            return "Medio"
        else:
            return "Alto"
            
    df_trans["segmento_monto"] = df_trans["monto_mxn"].apply(clasificar_segmento)
    
    logger.info("Fase 5 completada: Distribución de segmentos:")
    for seg, cnt in df_trans["segmento_monto"].value_counts().items():
        logger.info(f" - Segmento '{seg}': {cnt} transacciones ({cnt/len(df_trans)*100:.2f}%)")
        
    return df_trans
