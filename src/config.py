"""
MÓDULO DE CONFIGURACIÓN GLOBAL Y PARÁMETROS DEL PIPELINE FINTECH
================================================================
Proyecto Integrador Unidad 2: Limpieza y Preprocesamiento de Datos
Materia: Ingeniería de Datos (ATD-2602)
Tecnológico Nacional de México (TecNM) - Instituto Tecnológico de Colima
Catedrático: Prof. Armando Román Gallardo Heredia
Semestre: Agosto - Diciembre 2026 (2026-2)

Integrantes (Orden de Lista Oficial):
1. Benavides Manzo Fernando (Ingeniero 1: Calidad, Nulos y Outliers)
2. López Ordorica Ángel Alejandro (Ingeniero 2: Transformación e Integración)
3. Velázquez Gallardo Andrea Tairi (Ingeniera 3: Validación, Resiliencia y BD)
"""

import os
from pathlib import Path

# Rutas Base
SRC_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SRC_DIR.parent
PROJECT_ROOT = PIPELINE_DIR.parent
DATASETS_DIR = PIPELINE_DIR / "datasets"
DATABASE_DIR = PIPELINE_DIR / "database"
OUTPUTS_DIR = PIPELINE_DIR / "outputs"
CHARTS_DIR = OUTPUTS_DIR / "charts"
TESTS_DIR = PIPELINE_DIR / "tests"
DOCS_DIR = PIPELINE_DIR / "docs"
PRESENTACION_DIR = PROJECT_ROOT / "Presentación"

# Archivos de Datos Fuente
CATALOGO_CUENTAS_PATH = DATASETS_DIR / "catalogo_cuentas.csv"
TRANSACCIONES_ATM_PATH = DATASETS_DIR / "transacciones_atm.csv"
TRANSFERENCIAS_SPEI_PATH = DATASETS_DIR / "transferencias_digitales.json"

# Base de Datos SQLite
DB_PATH = DATABASE_DIR / "fintech_pipeline.db"

# Archivos de Salida de Auditoría
EXPORT_LIMPIAS_CSV = OUTPUTS_DIR / "transacciones_limpias_export.csv"
EXPORT_CUARENTENA_CSV = OUTPUTS_DIR / "cuarentena_errores_export.csv"
METRICAS_JSON_PATH = OUTPUTS_DIR / "metricas_calidad_antes_despues.json"

# Gráficas Estadísticas (300 DPI)
CHART_01_COMPLETITUD = CHARTS_DIR / "01_completitud_nulos_antes_despues.png"
CHART_02_BOXPLOT_IQR = CHARTS_DIR / "02_boxplot_outliers_iqr_capping.png"
CHART_03_DISTRIBUCION_SEGMENTOS = CHARTS_DIR / "03_distribucion_montos_segmentos.png"
CHART_04_EMBUDO_ETL = CHARTS_DIR / "04_embudo_etl_limpios_vs_cuarentena.png"

# Logos Oficiales
LOGO_TECNM_PATH = DOCS_DIR / "assets" / "logo_tecnm.png"
LOGO_ITCOLIMA_PATH = DOCS_DIR / "assets" / "logo_itcolima.png"

# Constantes de Negocio Bancario y Conversión
TIPO_CAMBIO_USD_MXN = 19.50  # Paridad fija oficial establecida por la institución bancaria
IQR_FACTOR = 1.5             # Factor multiplicativo estándar de Tukey para Rango Intercuartílico

# Etiquetas e Imputaciones Controladas
ID_CAJERO_DEFAULT = "ATM-NO-IDENTIFICADO"
EMAIL_IMPUTADO_DEFAULT = "anonimo@banco.com.mx"
CLABE_ATM_DEFAULT = "N/A_CAJERO_FISICO"
EMAIL_ATM_DEFAULT = "no_aplica_atm@banco.com.mx"

# Reglas de Discretización de Montos en Moneda Nacional (MXN)
# Basadas en normativa PLD/FT (Prevención de Lavado de Dinero) y análisis de cuantiles
UMBRAL_MONTO_BAJO_MAX = 5000.0     # Operaciones menores o micro-pagos
UMBRAL_MONTO_MEDIO_MAX = 25000.0   # Operaciones retail medianas habituales
# Montos superiores a 25,000.0 MXN se catalogan como 'Alto'

# Metadatos del Equipo para Portadas y Reportes
METADATOS_INSTITUCIONALES = {
    "institucion": "TECNOLÓGICO NACIONAL DE MÉXICO",
    "campus": "INSTITUTO TECNOLÓGICO DE COLIMA",
    "departamento": "Departamento de Sistemas y Computación",
    "carrera": "Ingeniería Informática / Sistemas",
    "materia": "Ingeniería de Datos (ATD-2602)",
    "unidad": "Unidad 2: Limpieza y Preprocesamiento de Datos",
    "docente": "Prof. Armando Román Gallardo Heredia",
    "periodo": "Agosto – Diciembre 2026 (Semestre 2026-2)",
    "ubicacion": "Villa de Álvarez, Colima, México",
    "grupo": "Grupo 2 (LCNT) – Fintech & Transacciones Bancarias (ATM + SPEI)",
    "equipo": [
        {
            "orden": 1,
            "nombre": "Benavides Manzo Fernando",
            "rol": "Ingeniero 1: Calidad, Nulos y Outliers",
            "especialidad": "Perfilado cuantitativo, deduplicación de cajero, análisis MCAR/MAR, imputación con mediana robusta y Capping IQR."
        },
        {
            "orden": 2,
            "nombre": "López Ordorica Ángel Alejandro",
            "rol": "Ingeniero 2: Transformación e Integración",
            "especialidad": "Integración multifuente CSV/JSON, estandarización temporal ISO-8601, paridad cambiaria USD->MXN, cruce Core Bancario y Min-Max."
        },
        {
            "orden": 3,
            "nombre": "Velázquez Gallardo Andrea Tairi",
            "rol": "Ingeniera 3: Validación, Resiliencia y Base de Datos",
            "especialidad": "Modelado Pydantic v2, motor de segregación Zero-Crash, persistencia transaccional SQLite ACID y pruebas de estrés."
        }
    ]
}

# Colores Institucionales
COLOR_TECNM_AZUL = "#1B396A"
COLOR_ITCOLIMA_VERDE = "#008080"
COLOR_DORADO = "#B38E5D"
COLOR_CUARENTENA_ROJO = "#C0392B"
COLOR_GRIS_PIZARRA = "#2C3E50"
COLOR_FONDO_CLARO = "#F8F9FA"
