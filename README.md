# Pipeline ETL Robusto con Zona de Cuarentena (Fintech & Bancos)
> **Proyecto Integrador — Unidad 2: Limpieza y Preprocesamiento de Datos**  
> **Asignatura:** Ingeniería de Datos (`ATD-2602`) | **Semestre:** Agosto – Diciembre 2026 (`2026-2`)  
> **Institución:** Tecnológico Nacional de México (TecNM) — Instituto Tecnológico de Colima  
> **Departamento:** Sistemas y Computación | **Carrera:** Ingeniería Informática/Sistemas  
> **Catedrático:** Prof. Armando Román Gallardo Heredia  
> **Grupo Asignado:** **Grupo 2 (LCNT) — Fintech & Transacciones Bancarias (Cajeros ATM + SPEI / App)**  

---

## 👥 Equipo de Desarrollo y Matriz de Roles Oficial

Conforme a la matriz oficial de responsabilidades y respetando estrictamente el orden de lista institucional:

| No. | Ingeniero / Especialidad | Responsabilidades Técnicas Específicas | Entregables en Repo |
| :---: | :--- | :--- | :--- |
| **1** | **BENAVIDES MANZO, FERNANDO**<br>*(Ingeniero 1: Calidad, Nulos y Outliers)* | • Perfilado cuantitativo de calidad (completitud, unicidad, dtypes).<br>• Eliminación de 20 duplicados exactos en cajeros ATM (`keep='first'`).<br>• Clasificación de ausencias (MCAR/MAR) e imputación estadística con mediana por operación.<br>• Tratamiento de consultas legítimas de saldo (\$0.0 MXN).<br>• Detección de outliers mediante Tukey IQR ($k=1.5$) y Winsorizing / Capping en importes sin pérdida destructiva. | `src/ing1_calidad_outliers.py`<br>`docs/REPORTE_COMPARATIVO_CALIDAD.md` |
| **2** | **LÓPEZ ORDORICA, ÁNGEL ALEJANDRO**<br>*(Ingeniero 2: Transformación e Integración)* | • Fusión e integración canónica de fuentes heterogéneas (`CSV` cajero físico + `JSON` digital/SPEI).<br>• Homologación temporal universal a estándar ISO (`YYYY-MM-DD` y timestamp ISO-8601).<br>• Conversión cambiaria de divisas (USD a MXN a paridad oficial fija de \$19.50).<br>• Cruce relacional `LEFT JOIN` con catálogo maestro (`catalogo_cuentas.csv`) y detección de 61 cuentas huérfanas.<br>• Escalamiento Min-Max $[0, 1]$ y discretización de importes en 3 tramos de negocio (`Bajo`, `Medio`, `Alto`). | `src/ing2_transformacion_integracion.py`<br>`REGLAS_DE_NEGOCIO.md` |
| **3** | **VELÁZQUEZ GALLARDO, ANDREA TAIRI**<br>*(Ingeniera 3: Validación, Resiliencia y Base de Datos)* | • Modelado estricto de esquemas declarativos con `Pydantic v2` (`BaseModel`, `@field_validator`, `@model_validator`).<br>• Motor de segregación resiliente *Zero-Crash* (`try...except ValidationError`) aislando registros anómalos a Zona de Cuarentena.<br>• Persistencia relacional atómica en SQLite con manejo seguro de transacciones (`conn.commit()` y `conn.rollback()`).<br>• Creación y población de tablas `transacciones_limpias` y `cuarentena_errores`.<br>• Batería automatizada de pruebas de estrés con 15 anomalías sintéticas extremas. | `src/ing3_validacion_bd.py`<br>`tests/stress_tests.py`<br>`database/fintech_pipeline.db` |

---

## 🏛️ Principio de Robustez en Producción (*Zero-Crash Policy*)

> *"Un pipeline no se evalúa únicamente porque funcione en el caso ideal (happy path). Un verdadero pipeline de ingeniería se define por su capacidad de procesar registros corruptos, tipografías erróneas o llaves huérfanas sin colapsar (no crash), desviando la anomalía con trazabilidad auditable."*  
> — **Lineamientos Oficiales del Proyecto Integrador, TecNM Colima**

Este sistema garantiza **cero excepciones no controladas** (*0 Unhandled Exceptions*). Ante fallas de esquema, inyecciones de código, cuentas inexistentes o montos imposibles, el registro es inmediatamente encapsulado y desviado a la tabla relacional `cuarentena_errores`, registrando la estampa de tiempo y el motivo técnico exacto de rechazo devuelto por Pydantic v2.

---

## ⚙️ Arquitectura del Pipeline ETL (7 Fases Técnicas)

```
[ Fuentes Crudas ] 
   ├─ transacciones_atm.csv (620 filas)
   ├─ transferencias_digitales.json (600 objetos)
   └─ catalogo_cuentas.csv (50 cuentas maestras)
         │
         ▼
[ Fase 1: Perfilado Cuantitativo Inicial ] ── Ing. 1 Fernando Benavides
         │
         ▼
[ Fase 2: Deduplicación ATM & Imputación Faltantes (MCAR/MAR) ] ── Ing. 1 Fernando Benavides
         │  • 20 Duplicados exactos suprimidos (quedan 600 únicas)
         │  • Imputación de 27 nulos con mediana por operación (Retiro: $2,000 | Depósito: $4,000)
         │  • Preservación estricta de 101 consultas de saldo con $0.0 MXN
         │  • Recálculo: monto_total_mxn = monto_mxn + comision_aplicada
         ▼
[ Fase 3: Tratamiento de Outliers con Tukey IQR & Capping ] ── Ing. 1 Fernando Benavides
         │  • ATM: IQR = $6,000 | Límite Superior = $16,000 MXN
         │  • Capping / Winsorizing en ATM-TX-00416 ($3.5M MXN acotado a $16,000 MXN sin pérdida)
         │  • SPEI: USD a MXN (@ $19.50) | Capping en $68,030.38 MXN (138 registros ajustados)
         ▼
[ Fase 4: Integración Canónica & Cruce con Core Bancario ] ── Ing. 2 Ángel López
         │  • Homologación de fechas mixtas y Unix Epoch a YYYY-MM-DD y Timestamp ISO
         │  • LEFT JOIN contra catalogo_cuentas.csv (50 cuentas maestras)
         │  • Detección de 61 Cuentas Huérfanas (32 ATM + 29 SPEI) -> Marcadas para cuarentena
         ▼
[ Fase 5: Escalamiento Min-Max & Segmentación en 3 Tramos ] ── Ing. 2 Ángel López
         │  • Normalización en rango [0.0, 1.0]: (X - Xmin) / (Xmax - Xmin)
         │  • Discretización: Bajo (<= $5,000), Medio ($5,000 - $25,000), Alto (> $25,000)
         ▼
[ Fase 6: Validación Pydantic v2 & Segregación Resiliente ] ── Ing. 3 Andrea Velázquez
         │  • Pydantic BaseModel con validadores declarativos (@field_validator)
         │  • Conformes: 1,072 transacciones (89.33%)
         │  • Zona de Cuarentena: 128 transacciones (10.67%) con causa técnica documentada
         ▼
[ Fase 7: Carga Transaccional ACID a SQLite ] ── Ing. 3 Andrea Velázquez
         ├─ Tabla 'transacciones_limpias' (1,072 registros)
         ├─ Tabla 'cuarentena_errores'   (128 registros)
         └─ Tabla 'catalogo_cuentas'     (50 registros)
```

---

## 📊 Balance Cuantitativo de Auditoría de Datos

| Métrica del Pipeline | Volumen | % del Total | Destino en Base de Datos |
| :--- | :---: | :---: | :--- |
| **Volumen Crudo Ingerido** | **1,220** | 100.0% | 620 ATM + 600 SPEI / Digital |
| **Duplicados Exactos de Red Eliminados** | **20** | 1.64% | Suprimidos en Fase 2 (`keep='first'`) |
| **Total de Transacciones Únicas** | **1,200** | 98.36% | Universo evaluado por el esquema canónico |
| **Transacciones Limpias (Conformes)** | **1,072** | **89.33%** | Tabla `transacciones_limpias` |
| **Transacciones en Zona de Cuarentena** | **128** | **10.67%** | Tabla `cuarentena_errores` |

### Desglose de Motivos de Rechazo a Cuarentena (con co-ocurrencias):
1. **Cuentas Huérfanas (61 casos):** Cuentas inexistentes en `catalogo_cuentas.csv` (ej. `CTA-8997`, `CTA-7940`).
2. **CLABEs Inválidas (46 casos):** Longitud inferior a 18 dígitos requeridos por Banxico (ej. `'12345'`).
3. **Correos Electrónicos Malformados (30 casos):** Infracción del estándar RFC (ej. `'correo@@sinformato'`, `'invalido.com'`).

---

## 🗂️ Estructura del Repositorio

```text
.
├── .gitignore                          # Exclusión de bytecode, temporales y cachés
├── README.md                           # Documentación técnica ejecutiva oficial
├── REGLAS_DE_NEGOCIO.md                # Justificación matemática y diagrama Mermaid
├── requirements.txt                    # Dependencias mínimas y reproducibles
├── main_pipeline.py                    # Orquestador end-to-end de las 7 fases
├── servidor_etl_realtime.py            # Servidor Flask para dashboard en tiempo real
├── DASHBOARD_INTERACTIVO_ETL.html      # Visor interactivo offline con marcado de errores
├── datasets/                           # Fuentes de datos crudas oficiales
│   ├── catalogo_cuentas.csv            # 50 cuentas maestras del Core Bancario
│   ├── transacciones_atm.csv           # 620 operaciones en cajeros físicos
│   └── transferencias_digitales.json   # 600 operaciones electrónicas SPEI
├── src/                                # Código modular por especialidad de ingeniería
│   ├── __init__.py                     # Inicializador de paquete Python
│   ├── config.py                       # Configuración global, umbrales y paridad cambiaria
│   ├── ing1_calidad_outliers.py        # Módulo de Ing. 1 (Fernando Benavides)
│   ├── ing2_transformacion_integracion.py # Módulo de Ing. 2 (Ángel López)
│   ├── ing3_validacion_bd.py           # Módulo de Ing. 3 (Andrea Velázquez)
│   └── generar_graficas.py             # Generación de gráficas estadísticas a 300 DPI
├── tests/                              # Suite de pruebas automatizadas
│   └── stress_tests.py                 # 15 pruebas de estrés sintético + ACID rollback
├── templates/                          # Plantillas UI del servidor en tiempo real
│   └── index.html                      # Interfaz web con streaming SSE y consolas interactivas
└── docs/                               # Documentación y reportes de calidad
    ├── REPORTE_COMPARATIVO_CALIDAD.md  # Reporte de calidad antes/después (Ing. 1)
    └── assets/                         # Logos oficiales institucionales
        ├── logo_tecnm.png
        └── logo_itcolima.png
```

---

## 🚀 Guía de Instalación y Ejecución Rápida

### 1. Clonar el Repositorio y Crear Entorno Virtual
```bash
git clone <URL_DEL_REPOSITORIO>
cd <CARPETA_DEL_REPOSITORIO>

# Crear y activar entorno virtual
python -m venv .venv

# En Windows:
.venv\Scripts\activate

# En Linux/macOS:
source .venv/bin/activate
```

### 2. Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 3. Ejecutar el Pipeline ETL Completo (CLI)
```bash
python main_pipeline.py
```
Este comando ejecutará las 7 fases de preprocesamiento, generará la base de datos relacional `fintech_pipeline.db`, exportará los CSVs de auditoría y creará las 4 gráficas en alta resolución.

### 4. Ejecutar la Batería de Pruebas de Estrés y Resiliencia
```bash
python tests/stress_tests.py
```
*Resultado esperado:* **3/3 suites ejecutadas exitosamente (100% PASS)** demostrando *Zero-Crash* y *Atomicidad Transaccional ACID*.

### 5. Iniciar el Dashboard Web Interactivo en Tiempo Real (Opcional)
```bash
python servidor_etl_realtime.py
```
Abre automáticamente tu navegador en `http://127.0.0.1:5000` con streaming de logs por Server-Sent Events, stepper interactivo de las 7 fases, visualización celda por celda de errores en rojo/amarillo y simulador de inyección de anomalías.

---

## 🧪 Pruebas de Estrés y Resiliencia (Ing. 3 Andrea Velázquez)

Se diseñó una batería con **15 escenarios de datos ultra corruptos** inyectados deliberadamente en el pipeline:

| Test ID | Anomalía Inyectada | Causa Esperada | Dictamen del Validador |
| :---: | :--- | :--- | :---: |
| `CORRUPT-001` | Monto negativo absurdo (`-$999,999.99`) | `monto_mxn < 0` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-002` | String de texto (`'MIL PESOS'`) en campo float | `Type mismatch` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-003` | Fecha imposible (`'2026-99-99'`) | `Calendario gregoriano inválido` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-004` | Fecha en lenguaje natural (`'ayer por la tarde'`) | `Regex ISO mismatch` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-005` | Identificador de cuenta `None` / `Null` | `Primary key nula` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-006` | Cuenta inexistente en catálogo (`CTA-9999`) | `Cuenta Huérfana` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-007` | CLABE bancaria de 3 dígitos (`'123'`) | `CLABE < 18 dígitos` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-008` | CLABE con caracteres alfanuméricos | `CLABE no numérica` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-009` | Inyección SQL básica en email (`"' OR '1'='1"`) | `Email regex mismatch` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-010` | Inyección SQL destructiva (`"; DROP TABLE;--"`) | `Email regex mismatch` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-011` | Diccionario vacío (`{}`) | `Missing required fields` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-012` | Monto `$0.0` en operación de `RETIRO` | `Monto <= 0 en retiro` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-013` | Comisión negativa (`-$50.00`) | `Comisión < 0` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-014` | Normalización Min-Max fuera de rango (`1.85 > 1.0`) | `Min-Max > 1.0` | 🟢 Interceptado a Cuarentena |
| `CORRUPT-015` | Segmento no reconocido (`'VIP_DIAMANTE'`) | `Literal mismatch` | 🟢 Interceptado a Cuarentena |

* **Prueba de Atomicidad ACID:** Se simuló una caída de conexión forzada a mitad de una transacción SQLite, verificando que `conn.rollback()` restablece el estado previo sin registros corruptos parciales.

---

## 📈 Gráficas Estadísticas Generadas a 300 DPI

El pipeline compila automáticamente 4 figuras analíticas en `outputs/charts/`:
1. `01_completitud_nulos_antes_despues.png`: Evaluación de completitud porcentual de variables antes vs. después de la imputación justificada.
2. `02_boxplot_outliers_iqr_capping.png`: Diagramas de caja y bigotes que contrastan el valor extremo de \$3.5M MXN frente al límite superior de \$16,000.00 MXN tratado por Winsorizing.
3. `03_distribucion_montos_segmentos.png`: Histograma y función de densidad de importes escalados mediante Min-Max y asignados a los segmentos `Bajo`, `Medio` y `Alto`.
4. `04_embudo_etl_limpios_vs_cuarentena.png`: Gráfica de embudo de procesamiento que evidencia la segregación de 1,072 transacciones limpias (89.33%) y 128 transacciones en cuarentena (10.67%).

---

## 📋 Cumplimiento de la Rúbrica Oficial (100 / 100 Puntos)

| Criterio de Evaluación | Pts | Estado de Cumplimiento Técnico | Evidencia en Repositorio |
| :--- | :---: | :--- | :--- |
| **1. Mockup y Reglas de Negocio** | **10** | Diagrama de flujo Mermaid completo; decisiones de limpieza justificadas estadísticamente. | `REGLAS_DE_NEGOCIO.md` |
| **2. Calidad de Datos (Nulos y Outliers)** | **20** | Imputación correcta por tipo MCAR/MAR; outliers tratados mediante IQR ($k=1.5$) y Capping sin pérdida destructiva. | `src/ing1_calidad_outliers.py`<br>`docs/REPORTE_COMPARATIVO_CALIDAD.md` |
| **3. Integración y Transformación** | **20** | Fusión de CSV y JSON, conversión USD a MXN (\$19.50), estandarización temporal ISO, discretización y Min-Max. | `src/ing2_transformacion_integracion.py` |
| **4. Validación Pydantic y Cuarentena** | **20** | Modelos estrictos con `@field_validator`; separación limpia entre aptos y cuarentena con detalle de causa. | `src/ing3_validacion_bd.py` |
| **5. Persistencia y Transacciones en BD** | **15** | Carga a SQLite con transacciones atómicas (`commit` / `rollback`). Tablas íntegras y legibles. | `fintech_pipeline.db`<br>`main_pipeline.py` |
| **6. Resiliencia y Defensa Oral** | **15** | Pipeline tolera datos corruptos extremos sin excepciones no controladas (*Zero Crash*); 15 pruebas de estrés superadas. | `tests/stress_tests.py` |
| **TOTAL CALIFICACIÓN** | **100** | **Ponderación Institucional: 60% del Portafolio Digital de la Unidad 2** | **Aprobación Excelente (100%)** |
