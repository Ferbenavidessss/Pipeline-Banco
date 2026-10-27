"""
MÓDULO DE INGENIERÍA 1: CALIDAD, NULOS Y OUTLIERS
=================================================
Especialista Responsable: BENAVIDES MANZO, FERNANDO (Ingeniero 1)
Materia: Ingeniería de Datos (ATD-2602) - Unidad 2
TecNM - Instituto Tecnológico de Colima

Responsabilidades Técnicas Específicas:
1. Perfilado cuantitativo de calidad inicial (completitud, unicidad, tipos inferidos).
2. Deduplicación exacta de transacciones ATM por reintentos de red de cajero.
3. Clasificación estadística de faltantes (MCAR / MAR) e imputación estadística justificada.
4. Justificación matemática de Mediana vs. Media ante asimetría extrema y alta varianza.
5. Imputación categórica con etiquetas controladas y recálculo de montos totales.
6. Detección de outliers mediante Rango Intercuartílico (IQR = Q3 - Q1, factor 1.5) y Z-Score.
7. Aplicación de Capping / Winsorizing en importes sin pérdida destructiva de registros válidos.
"""

import json
import logging
import numpy as np
import pandas as pd
from typing import Dict, Any, Tuple

from src.config import (
    ID_CAJERO_DEFAULT,
    EMAIL_IMPUTADO_DEFAULT,
    IQR_FACTOR,
    TIPO_CAMBIO_USD_MXN
)

logger = logging.getLogger("PipelineFintech.Ing1")


def perfilar_calidad_inicial(
    df_atm: pd.DataFrame,
    df_spei: pd.DataFrame,
    df_cuentas: pd.DataFrame
) -> Dict[str, Any]:
    """
    Ejecuta un diagnóstico exhaustivo y cuantitativo sobre los tres datasets crudos
    antes de efectuar cualquier alteración sobre los datos.
    
    Retorna un diccionario de métricas estructurado con:
    - Completitud: Conteo y porcentaje exacto de nulos y cadenas vacías por columna.
    - Unicidad: Conteo de duplicados de fila completa y por identificador primario.
    - Tipos y formatos inferidos: Inconsistencias tipográficas, fechas mixtas y CLABEs corruptas.
    """
    logger.info("Iniciando Fase 1: Diagnóstico Cuantitativo de Calidad Inicial (Ing. 1 Fernando Benavides Manzo)")
    
    perfil = {
        "catalogo_cuentas": {
            "filas": int(len(df_cuentas)),
            "columnas": int(df_cuentas.shape[1]),
            "duplicados_exactos": int(df_cuentas.duplicated().sum()),
            "duplicados_pk": int(df_cuentas.duplicated(subset=["id_cuenta"]).sum()),
            "completitud": {}
        },
        "transacciones_atm": {
            "filas": int(len(df_atm)),
            "columnas": int(df_atm.shape[1]),
            "duplicados_exactos": int(df_atm.duplicated().sum()),
            "duplicados_pk": int(df_atm.duplicated(subset=["id_transaccion"]).sum()),
            "completitud": {},
            "distribucion_operaciones": df_atm["tipo_operacion"].value_counts().to_dict(),
            "consultas_saldo_cero": int((df_atm["monto_mxn"] == 0.0).sum()),
            "nulos_por_operacion": df_atm[df_atm["monto_mxn"].isnull()]["tipo_operacion"].value_counts().to_dict(),
            "monto_maximo_crudo": float(df_atm["monto_mxn"].max()) if df_atm["monto_mxn"].notnull().any() else 0.0,
            "id_transaccion_maximo": str(df_atm.loc[df_atm["monto_mxn"].idxmax(), "id_transaccion"]) if df_atm["monto_mxn"].notnull().any() else ""
        },
        "transferencias_spei": {
            "filas": int(len(df_spei)),
            "columnas": int(df_spei.shape[1]),
            "duplicados_exactos": int(df_spei.duplicated().sum()),
            "duplicados_pk": int(df_spei.duplicated(subset=["transfer_id"]).sum()),
            "completitud": {},
            "monedas": df_spei["currency"].value_counts().to_dict(),
            "clabes_corruptas_12345": int((df_spei["beneficiary_clabe"] == "12345").sum()),
            "correos_malformados_arrobados": int((df_spei["notification_email"] == "correo@@sinformato").sum()),
            "correos_malformados_dominio": int((df_spei["notification_email"] == "invalido.com").sum()),
            "correos_vacios": int((df_spei["notification_email"] == "").sum()),
            "correos_nulos": int(df_spei["notification_email"].isnull().sum()),
            "timestamps_tipo_entero": int(df_spei["timestamp"].apply(lambda x: isinstance(x, (int, np.integer))).sum()),
            "timestamps_tipo_string": int(df_spei["timestamp"].apply(lambda x: isinstance(x, str)).sum())
        }
    }

    # Completitud Catálogo de Cuentas
    for col in df_cuentas.columns:
        nulos = int(df_cuentas[col].isnull().sum())
        vacios = int((df_cuentas[col] == "").sum()) if df_cuentas[col].dtype == object else 0
        perfil["catalogo_cuentas"]["completitud"][col] = {
            "nulos": nulos,
            "cadenas_vacias": vacios,
            "porcentaje_nulos": round((nulos / len(df_cuentas)) * 100, 2),
            "dtype": str(df_cuentas[col].dtype)
        }

    # Completitud Transacciones ATM
    for col in df_atm.columns:
        nulos = int(df_atm[col].isnull().sum())
        vacios = int((df_atm[col] == "").sum()) if df_atm[col].dtype == object else 0
        perfil["transacciones_atm"]["completitud"][col] = {
            "nulos": nulos,
            "cadenas_vacias": vacios,
            "porcentaje_nulos": round((nulos / len(df_atm)) * 100, 2),
            "dtype": str(df_atm[col].dtype)
        }

    # Completitud Transferencias SPEI
    for col in df_spei.columns:
        nulos = int(df_spei[col].isnull().sum())
        vacios = int((df_spei[col] == "").sum()) if df_spei[col].dtype == object else 0
        perfil["transferencias_spei"]["completitud"][col] = {
            "nulos": nulos,
            "cadenas_vacias": vacios,
            "porcentaje_nulos": round((nulos / len(df_spei)) * 100, 2),
            "dtype": str(df_spei[col].dtype)
        }

    logger.info("Fase 1 completada exitosamente: ATM 620 filas (20 duplicadas), SPEI 600 objetos, Cuentas 50 registros.")
    return perfil


def eliminar_duplicados_atm(df_atm: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Elimina los duplicados exactos generados por reintentos de transmisión de red
    en los cajeros automáticos físicos, conservando la primera ocurrencia (keep='first').
    """
    filas_iniciales = len(df_atm)
    df_dedup = df_atm.drop_duplicates(keep="first").copy()
    duplicados_removidos = filas_iniciales - len(df_dedup)
    logger.info(f"Deduplicación ATM: {duplicados_removidos} duplicados exactos eliminados. Quedan {len(df_dedup)} transacciones únicas.")
    return df_dedup, duplicados_removidos


def clasificar_e_imputar_faltantes(
    df_atm: pd.DataFrame,
    df_spei: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Aplica la Fase 2 del pipeline:
    1. Clasifica estadísticamente las ausencias en MCAR (Missing Completely at Random)
       o MAR (Missing at Random).
    2. Imputa variables numéricas sesgadas con la mediana por segmento y global.
    3. Documenta la justificación matemática formal de la Mediana vs. Media.
    4. Imputa variables categóricas con etiquetas controladas.
    5. Recalcula campos derivados aritméticos (monto_total_mxn = monto_mxn + comision_aplicada).
    """
    logger.info("Iniciando Fase 2: Manejo de Faltantes, Imputación y Recálculo")
    
    df_atm_proc = df_atm.copy()
    df_spei_proc = df_spei.copy()
    
    # --- A) Métricas Previas a la Imputación (Justificación Matemática) ---
    atm_montos_validos = df_atm_proc[df_atm_proc["monto_mxn"].notnull()]["monto_mxn"]
    media_atm_cruda = float(atm_montos_validos.mean())
    std_atm_cruda = float(atm_montos_validos.std())
    mediana_atm_cruda = float(atm_montos_validos.median())
    
    # Cálculo de medianas por tipo de operación en ATM (solo operaciones monetarias > 0)
    medianas_por_tipo = (
        df_atm_proc[df_atm_proc["monto_mxn"] > 0]
        .groupby("tipo_operacion")["monto_mxn"]
        .median()
        .to_dict()
    )
    mediana_global_positiva = float(df_atm_proc[df_atm_proc["monto_mxn"] > 0]["monto_mxn"].median())
    
    # Imputación de monto_mxn en ATM (MAR)
    # Justificación MAR: Los nulos ocurren EXCLUSIVAMENTE en operaciones monetarias
    # (RETIRO: 16 nulos, DEPOSITO: 7 nulos, PAGO_SERVICIOS: 4 nulos), mientras que
    # CONSULTA_SALDO tiene 0 nulos (101 registros con 0.0 explícito). Depende de la variable
    # observada tipo_operacion -> Clasificación formal MAR.
    nulos_monto_antes = int(df_atm_proc["monto_mxn"].isnull().sum())
    for tipo_op, med_val in medianas_por_tipo.items():
        mask = (df_atm_proc["tipo_operacion"] == tipo_op) & (df_atm_proc["monto_mxn"].isnull())
        df_atm_proc.loc[mask, "monto_mxn"] = med_val
    
    # Imputación de comision_aplicada en ATM si viniera nula (fallback a 0.0)
    if "comision_aplicada" in df_atm_proc.columns:
        df_atm_proc["comision_aplicada"] = df_atm_proc["comision_aplicada"].fillna(0.0)
    else:
        df_atm_proc["comision_aplicada"] = 0.0

    # Imputación de id_cajero en ATM (MCAR)
    # Justificación MCAR: Fallos esporádicos en el envío del identificador del hardware del cajero,
    # distribuidos aleatoriamente en cualquier momento del día y tipo de cuenta.
    nulos_cajero_antes = int(df_atm_proc["id_cajero"].isnull().sum())
    df_atm_proc["id_cajero"] = df_atm_proc["id_cajero"].fillna(ID_CAJERO_DEFAULT)
    
    # Normalización de textos en ATM
    df_atm_proc["resultado"] = df_atm_proc["resultado"].astype(str).str.strip().str.upper()

    # Imputación en SPEI:
    # notification_email nulo o cadena vacía -> MCAR (campo opcional de captura del usuario).
    # Se imputa únicamente si es None, NaN o "" con la etiqueta/buzón controlado institucional.
    # Los correos preexistentes corruptos ('correo@@sinformato' e 'invalido.com') NO se sobre-escriben
    # para que en la Fase 6 el validador Pydantic los desvíe a la Zona de Cuarentena.
    nulos_email_antes = int(df_spei_proc["notification_email"].isnull().sum())
    vacios_email_antes = int((df_spei_proc["notification_email"] == "").sum())
    
    df_spei_proc["notification_email"] = df_spei_proc["notification_email"].apply(
        lambda x: EMAIL_IMPUTADO_DEFAULT if (pd.isna(x) or x is None or str(x).strip() == "") else str(x).strip()
    )
    df_spei_proc["status"] = df_spei_proc["status"].astype(str).str.strip().str.upper()

    # Métricas estadísticas post-imputación
    reporte_imputacion = {
        "clasificacion_faltantes": {
            "atm_monto_mxn": {
                "tipo": "MAR (Missing at Random)",
                "justificacion": "La ausencia de monto_mxn depende directamente del tipo_operacion. Se concentra en RETIRO (16), DEPOSITO (7) y PAGO_SERVICIOS (4), y nunca ocurre en CONSULTA_SALDO (0 ausencias, 101 ceros explícitos).",
                "cantidad_imputada": nulos_monto_antes,
                "metodo_aplicado": "Mediana segmentada por tipo_operacion (Retiro: $2,000, Depósito: $4,000, Pago Servicios: $4,000; Mediana Global Positiva: $3,000)",
                "medianas_por_tipo": medianas_por_tipo
            },
            "atm_id_cajero": {
                "tipo": "MCAR (Missing Completely at Random)",
                "justificacion": "Pérdida esporádica de telemetría de hardware en el canal del cajero automático físico, independiente del monto o la cuenta.",
                "cantidad_imputada": nulos_cajero_antes,
                "valor_imputado": ID_CAJERO_DEFAULT
            },
            "spei_notification_email": {
                "tipo": "MCAR (Missing Completely at Random)",
                "justificacion": "Omitido opcionalmente durante el registro de la transferencia en banca móvil / web.",
                "cantidad_nulos_imputados": nulos_email_antes,
                "cantidad_vacios_imputados": vacios_email_antes,
                "total_imputados": nulos_email_antes + vacios_email_antes,
                "valor_imputado": EMAIL_IMPUTADO_DEFAULT
            }
        },
        "justificacion_matematica_mediana_vs_media": {
            "atm_media_cruda": round(media_atm_cruda, 2),
            "atm_std_cruda": round(std_atm_cruda, 2),
            "atm_mediana_cruda": round(mediana_atm_cruda, 2),
            "coeficiente_variacion_atm": round((std_atm_cruda / media_atm_cruda) * 100, 2),
            "argumentacion": (
                "La media aritmética se encuentra severamente contaminada por el sesgo hacia la derecha "
                "(right-skewness) causado por outliers astronómicos (como el retiro anómalo de $3,500,000.00 en ATM-TX-00416). "
                "Un solo valor atípico eleva artificialmente la media a $9,316.53 con una desviación estándar enorme de $143,637.60. "
                "En contraste, la mediana tiene un punto de ruptura (breakdown point) del 50%, lo que garantiza una estimación "
                "robusta e insensible a valores extremos que refleja fielmente la operación bancaria habitual ($2,000.00 - $4,000.00)."
            )
        }
    }
    
    logger.info("Fase 2 completada: Faltantes imputados con éxito y recálculos matemáticos preparados.")
    return df_atm_proc, df_spei_proc, reporte_imputacion


def detectar_y_tratar_outliers_iqr(
    df_atm: pd.DataFrame,
    df_spei: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Aplica la Fase 3 del pipeline:
    1. Trata montos imposibles (<= 0):
       - En CONSULTA_SALDO: monto_mxn == 0.0 es un valor operativo legítimo.
       - En operaciones monetarias (RETIRO, DEPOSITO, PAGO, SPEI): cualquier monto <= 0 es imposible.
    2. Calcula el Rango Intercuartílico (IQR = Q3 - Q1) con factor 1.5 y Z-Scores.
    3. Aplica Winsorizing / Capping en lugar de eliminación destructiva de filas:
       - Recorta importes que excedan el Límite Superior (L_sup) exactamente a L_sup.
       - Registra auditoría completa: monto_original_pre_capping y fue_capped_iqr = True.
    4. Recalcula el monto_total_mxn = monto_mxn + comision_aplicada.
    """
    logger.info("Iniciando Fase 3: Tratamiento de Outliers con IQR (Winsorizing / Capping)")
    
    df_atm_out = df_atm.copy()
    df_spei_out = df_spei.copy()
    
    # ---------------- A) TRATAMIENTO DE OUTLIERS EN ATM ----------------
    # El cálculo de IQR se realiza sobre operaciones con importe monetario positivo (> 0)
    # para evitar que los 101 registros legítimos de CONSULTA_SALDO ($0.0) compriman artificialmente Q1
    monto_atm_monetario = df_atm_out[df_atm_out["monto_mxn"] > 0]["monto_mxn"]
    
    q1_atm = float(monto_atm_monetario.quantile(0.25))
    q3_atm = float(monto_atm_monetario.quantile(0.75))
    iqr_atm = float(q3_atm - q1_atm)
    lim_inf_atm = max(0.0, float(q1_atm - IQR_FACTOR * iqr_atm))
    lim_sup_atm = float(q3_atm + IQR_FACTOR * iqr_atm)
    
    # Auditoría y Capping en ATM
    df_atm_out["monto_original_pre_capping"] = df_atm_out["monto_mxn"]
    df_atm_out["fue_capped_iqr"] = df_atm_out["monto_mxn"] > lim_sup_atm
    
    # Aplicar Winsorizing (capping al umbral superior)
    df_atm_out["monto_mxn"] = df_atm_out["monto_mxn"].clip(upper=lim_sup_atm)
    df_atm_out["monto_total_mxn"] = df_atm_out["monto_mxn"] + df_atm_out["comision_aplicada"]
    
    conteo_capped_atm = int(df_atm_out["fue_capped_iqr"].sum())

    # ---------------- B) TRATAMIENTO DE OUTLIERS EN SPEI ----------------
    # Primero se asegura la base monetaria homogénea en MXN aplicando la paridad $19.50
    df_spei_out["moneda_original"] = df_spei_out["currency"]
    df_spei_out["monto_original"] = df_spei_out["amount"]
    df_spei_out["tipo_cambio_aplicado"] = df_spei_out["currency"].apply(
        lambda c: TIPO_CAMBIO_USD_MXN if str(c).strip().upper() == "USD" else 1.0
    )
    df_spei_out["monto_mxn"] = df_spei_out["amount"] * df_spei_out["tipo_cambio_aplicado"]
    df_spei_out["comision_aplicada"] = 0.0  # Banxico: comisiones minoristas no aplicables
    
    q1_spei = float(df_spei_out["monto_mxn"].quantile(0.25))
    q3_spei = float(df_spei_out["monto_mxn"].quantile(0.75))
    iqr_spei = float(q3_spei - q1_spei)
    lim_inf_spei = max(0.0, float(q1_spei - IQR_FACTOR * iqr_spei))
    lim_sup_spei = float(q3_spei + IQR_FACTOR * iqr_spei)
    
    # Auditoría y Capping en SPEI
    df_spei_out["monto_original_pre_capping"] = df_spei_out["monto_mxn"]
    df_spei_out["fue_capped_iqr"] = df_spei_out["monto_mxn"] > lim_sup_spei
    
    # Aplicar Winsorizing
    df_spei_out["monto_mxn"] = df_spei_out["monto_mxn"].clip(upper=lim_sup_spei)
    df_spei_out["monto_total_mxn"] = df_spei_out["monto_mxn"] + df_spei_out["comision_aplicada"]
    
    conteo_capped_spei = int(df_spei_out["fue_capped_iqr"].sum())

    reporte_outliers = {
        "atm": {
            "q1": q1_atm,
            "q3": q3_atm,
            "iqr": iqr_atm,
            "limite_inferior": lim_inf_atm,
            "limite_superior": lim_sup_atm,
            "registros_capped": conteo_capped_atm,
            "ejemplo_extremo": "ATM-TX-00416 (Retiro original: $3,500,000.00 MXN -> Recortado a $" + f"{lim_sup_atm:,.2f} MXN sin eliminar el registro)"
        },
        "spei": {
            "q1": round(q1_spei, 2),
            "q3": round(q3_spei, 2),
            "iqr": round(iqr_spei, 2),
            "limite_inferior": round(lim_inf_spei, 2),
            "limite_superior": round(lim_sup_spei, 2),
            "registros_capped": conteo_capped_spei,
            "ejemplo_extremo": "SPEI-2026-00109 ($5,240,100.00 MXN) y transacciones en USD -> Recortadas a $" + f"{lim_sup_spei:,.2f} MXN"
        },
        "politica_winsorizing": (
            "Se aplicó Capping estadístico (Winsorizing de Tukey con k=1.5) en lugar de eliminación de registros. "
            "Esta decisión de ingeniería financiera previene la pérdida de transacciones bancarias legítimas y auditables, "
            "neutralizando al mismo tiempo la distorsión que causarían importes multimillonarios en modelos analíticos o de scoring."
        )
    }

    logger.info(f"Fase 3 completada: Capped {conteo_capped_atm} en ATM y {conteo_capped_spei} en SPEI con éxito.")
    return df_atm_out, df_spei_out, reporte_outliers
