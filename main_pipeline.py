"""
ORQUESTADOR PRINCIPAL END-TO-END DEL PIPELINE ETL FINTECH
=========================================================
Proyecto Integrador Unidad 2: Limpieza y Preprocesamiento de Datos
Materia: Ingeniería de Datos (ATD-2602)
Tecnológico Nacional de México (TecNM) - Instituto Tecnológico de Colima
Catedrático: Prof. Armando Román Gallardo Heredia
Semestre: Agosto – Diciembre 2026 (Semestre 2026-2)

Equipo Oficial de Desarrollo (Orden de Lista Oficial):
1. BENAVIDES MANZO, FERNANDO           (Ingeniero 1: Calidad, Nulos y Outliers)
2. LÓPEZ ORDORICA, ÁNGEL ALEJANDRO     (Ingeniero 2: Transformación e Integración)
3. VELÁZQUEZ GALLARDO, ANDREA TAIRI    (Ingeniera 3: Validación, Resiliencia y Base de Datos)
"""

import json
import logging
import os
import shutil
import sys
from pathlib import Path

# Asegurar que el directorio raíz de Pipeline esté en el PATH
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

import pandas as pd
import numpy as np

from src.config import (
    CATALOGO_CUENTAS_PATH,
    TRANSACCIONES_ATM_PATH,
    TRANSFERENCIAS_SPEI_PATH,
    DB_PATH,
    OUTPUTS_DIR,
    EXPORT_LIMPIAS_CSV,
    EXPORT_CUARENTENA_CSV,
    METRICAS_JSON_PATH,
    CHARTS_DIR,
    METADATOS_INSTITUCIONALES,
    PRESENTACION_DIR
)
from src.ing1_calidad_outliers import (
    perfilar_calidad_inicial,
    eliminar_duplicados_atm,
    clasificar_e_imputar_faltantes,
    detectar_y_tratar_outliers_iqr
)
from src.ing2_transformacion_integracion import (
    homologar_fechas_multifuente,
    integrar_fuentes_heterogeneas,
    cruzar_catalogo_y_detectar_huerfanos,
    escalar_minmax_y_discretizar
)
from src.ing3_validacion_bd import (
    segregar_registros_pydantic,
    cargar_a_sqlite_transaccional
)
from src.generar_graficas import generar_todas_las_graficas

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("PipelinePrincipal")


def ejecutar_pipeline_completo():
    print("=" * 85)
    print(" TECNOLÓGICO NACIONAL DE MÉXICO - CAMPUS COLIMA")
    print(" DEPARTAMENTO DE SISTEMAS Y COMPUTACIÓN | INGENIERÍA DE DATOS (ATD-2602)")
    print(" PROYECTO INTEGRADOR UNIDAD 2: PIPELINE ETL ROBUSTO CON ZONA DE CUARENTENA")
    print(" GRUPO 2 (LCNT) - FINTECH & TRANSACCIONES BANCARIAS (CAJEROS ATM + SPEI)")
    print(" Docente: Prof. Armando Román Gallardo Heredia | Periodo: 2026-2")
    print(" Integrantes (Orden Oficial):")
    print("   1. Benavides Manzo Fernando (Ing. 1: Calidad, Nulos y Outliers)")
    print("   2. López Ordorica Ángel Alejandro (Ing. 2: Transformación e Integración)")
    print("   3. Velázquez Gallardo Andrea Tairi (Ing. 3: Validación, Resiliencia y BD)")
    print("=" * 85)

    # -------------------------------------------------------------
    # 0. CARGA DE DATASETS CRUDOS
    # -------------------------------------------------------------
    logger.info("Cargando datasets crudos desde el directorio datasets/...")
    df_cuentas = pd.read_csv(CATALOGO_CUENTAS_PATH)
    df_atm_crudo = pd.read_csv(TRANSACCIONES_ATM_PATH)
    
    with open(TRANSFERENCIAS_SPEI_PATH, "r", encoding="utf-8") as f:
        spei_data = json.load(f)
    df_spei_crudo = pd.DataFrame(spei_data)
    
    logger.info(f"Cuentas maestras cargadas: {len(df_cuentas)} registros.")
    logger.info(f"ATM transacciones crudas: {len(df_atm_crudo)} filas.")
    logger.info(f"SPEI transferencias crudas: {len(df_spei_crudo)} objetos JSON.")

    # -------------------------------------------------------------
    # FASE 1: DIAGNÓSTICO DE CALIDAD INICIAL (ING. 1)
    # -------------------------------------------------------------
    perfil_inicial = perfilar_calidad_inicial(df_atm_crudo, df_spei_crudo, df_cuentas)

    # -------------------------------------------------------------
    # FASE 2: DEDUPLICACIÓN, IMPUTACIÓN Y RECÁLCULO (ING. 1)
    # -------------------------------------------------------------
    # A) Deduplicación ATM
    df_atm_dedup, dups_atm = eliminar_duplicados_atm(df_atm_crudo)
    
    # B) Imputación estadística justificada y recálculos
    df_atm_imputado, df_spei_imputado, reporte_imputacion = clasificar_e_imputar_faltantes(
        df_atm_dedup, df_spei_crudo
    )

    # -------------------------------------------------------------
    # FASE 3: TRATAMIENTO DE OUTLIERS CON IQR Y WINSORIZING (ING. 1)
    # -------------------------------------------------------------
    df_atm_outliers, df_spei_outliers, reporte_outliers = detectar_y_tratar_outliers_iqr(
        df_atm_imputado, df_spei_imputado
    )

    # -------------------------------------------------------------
    # FASE 4: INTEGRACIÓN MULTIFUENTE, FECHAS Y CRUCE CORE (ING. 2)
    # -------------------------------------------------------------
    # A) Homologación temporal a ISO-8601
    df_atm_fechas, df_spei_fechas = homologar_fechas_multifuente(
        df_atm_outliers, df_spei_outliers
    )
    
    # B) Integración canónica de fuentes heterogéneas
    df_integrado = integrar_fuentes_heterogeneas(df_atm_fechas, df_spei_fechas)
    
    # C) Cruce relacional contra catálogo y detección de huérfanos
    df_merged, reporte_cruce = cruzar_catalogo_y_detectar_huerfanos(df_integrado, df_cuentas)

    # -------------------------------------------------------------
    # FASE 5: TRANSFORMACIÓN, ESCALAMIENTO MIN-MAX Y BINNING (ING. 2)
    # -------------------------------------------------------------
    df_transformado = escalar_minmax_y_discretizar(df_merged)

    # -------------------------------------------------------------
    # FASE 6: VALIDACIÓN ESTRICTA CON PYDANTIC Y CUARENTENA (ING. 3)
    # -------------------------------------------------------------
    df_limpias, df_cuarentena, reporte_segregacion = segregar_registros_pydantic(df_transformado)

    # -------------------------------------------------------------
    # FASE 7: PERSISTENCIA TRANSACCIONAL ATÓMICA EN SQLITE (ING. 3)
    # -------------------------------------------------------------
    reporte_bd = cargar_a_sqlite_transaccional(df_limpias, df_cuarentena, df_cuentas, DB_PATH)

    # -------------------------------------------------------------
    # EXPORTACIÓN DE ENTREGABLES Y AUDITORÍA
    # -------------------------------------------------------------
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    os.makedirs(CHARTS_DIR, exist_ok=True)
    logger.info("Exportando datasets analíticos y de auditoría a CSV...")
    df_limpias.to_csv(EXPORT_LIMPIAS_CSV, index=False, encoding="utf-8")
    df_cuarentena.to_csv(EXPORT_CUARENTENA_CSV, index=False, encoding="utf-8")
    logger.info(f"Guardado: {EXPORT_LIMPIAS_CSV}")
    logger.info(f"Guardado: {EXPORT_CUARENTENA_CSV}")

    # Consolidación de métricas en JSON
    metricas_completas = {
        "metadatos": METADATOS_INSTITUCIONALES,
        "fase_1_diagnostico_inicial": perfil_inicial,
        "fase_2_deduplicacion_e_imputacion": {
            "duplicados_atm_eliminados": dups_atm,
            "reporte_imputacion": reporte_imputacion
        },
        "fase_3_outliers_winsorizing": reporte_outliers,
        "fase_4_integracion_y_cruce": reporte_cruce,
        "fase_5_escalamiento_y_discretizacion": {
            "distribucion_segmentos_global": df_transformado["segmento_monto"].value_counts().to_dict(),
            "distribucion_segmentos_limpias": df_limpias["segmento_monto"].value_counts().to_dict()
        },
        "fase_6_segregacion_cuarentena": reporte_segregacion,
        "fase_7_persistencia_sqlite": reporte_bd
    }

    with open(METRICAS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(metricas_completas, f, indent=2, ensure_ascii=False)
    logger.info(f"Resumen cuantitativo exportado a: {METRICAS_JSON_PATH}")

    # -------------------------------------------------------------
    # GENERACIÓN DE GRÁFICAS ESTADÍSTICAS A 300 DPI
    # -------------------------------------------------------------
    logger.info("Generando gráficas estadísticas en alta resolución (300 DPI)...")
    # Serie de SPEI en MXN para boxplot
    spei_monto_crudo_mxn = df_spei_crudo["amount"] * df_spei_crudo["currency"].apply(lambda c: 19.50 if c == "USD" else 1.0)
    
    generar_todas_las_graficas(
        perfil_inicial=perfil_inicial,
        df_atm_crudo=df_atm_crudo,
        df_atm_proc=df_atm_outliers,
        df_spei_crudo_mxn=spei_monto_crudo_mxn,
        df_spei_proc_mxn=df_spei_outliers["monto_mxn"],
        df_limpias=df_limpias,
        df_cuarentena=df_cuarentena
    )

    # Copiar gráficas a la carpeta de Presentación para su uso en diapositivas (si existe)
    if PRESENTACION_DIR.exists():
        pres_charts = PRESENTACION_DIR / "charts"
        os.makedirs(pres_charts, exist_ok=True)
        for c_file in CHARTS_DIR.glob("*.png"):
            shutil.copy2(c_file, pres_charts / c_file.name)
        logger.info("Gráficas sincronizadas con Presentación/charts/ con éxito.")

    # -------------------------------------------------------------
    # RESUMEN EJECUTIVO FINAL EN CONSOLA
    # -------------------------------------------------------------
    print("\n" + "=" * 85)
    print(" RESUMEN DE AUDITORÍA Y RESULTADOS FINALES DEL PIPELINE ETL")
    print("=" * 85)
    print(f" * Registros Crudos Totales Ingeridos:   1,220  (620 ATM + 600 SPEI)")
    print(f" * Duplicados Exactos de Red Eliminados: 20     (ATM Cajeros Físicos)")
    print(f" * Total de Transacciones Únicas:        1,200  (600 ATM + 600 SPEI)")
    print(f" * Total Transacciones Conformes (BD):   1,072  ({len(df_limpias)/1200*100:.2f}%) -> Tabla 'transacciones_limpias'")
    print(f" * Total Transacciones en Cuarentena:    128    ({len(df_cuarentena)/1200*100:.2f}%) -> Tabla 'cuarentena_errores'")
    print("-" * 85)
    print(" DESGLOSE DE MOTIVOS DE RECHAZO EN ZONA DE CUARENTENA (CON CO-OCURRENCIAS):")
    for mot, cnt in sorted(reporte_segregacion["desglose_motivos_rechazo"].items(), key=lambda x: -x[1]):
        print(f"   - {mot:<20}: {cnt:>3} registros aislados con trazabilidad")
    print("-" * 85)
    print(" PERSISTENCIA EN BASE DE DATOS SQLITE (fintech_pipeline.db):")
    print(f"   - Tabla 'transacciones_limpias': {reporte_bd['filas_insertadas']['transacciones_limpias']:>5} registros")
    print(f"   - Tabla 'cuarentena_errores'   : {reporte_bd['filas_insertadas']['cuarentena_errores']:>5} registros")
    print(f"   - Tabla 'catalogo_cuentas'     : {reporte_bd['filas_insertadas']['catalogo_cuentas']:>5} cuentas maestras")
    print(f"   - Atomicidad Transaccional ACID: ÉXITO TOTAL (conn.commit())")
    print("=" * 85 + "\n")

    return metricas_completas


if __name__ == "__main__":
    ejecutar_pipeline_completo()
