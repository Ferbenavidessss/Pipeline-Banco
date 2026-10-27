# REPORTE COMPARATIVO DE CALIDAD DE DATOS (ANTES VS. DESPUÉS)
## ESPECIALIDAD DE INGENIERÍA 1: CALIDAD, NULOS Y OUTLIERS

---

### DATOS ACADÉMICOS E INSTITUCIONALES
* **Institución:** Tecnológico Nacional de México (TecNM) – Instituto Tecnológico de Colima
* **Materia:** Ingeniería de Datos (`ATD-2602`) | **Unidad 2:** Limpieza y Preprocesamiento de Datos
* **Docente:** Prof. Armando Román Gallardo Heredia | **Semestre:** 2026-2
* **Especialista Responsable:** **BENAVIDES MANZO, FERNANDO** (Ingeniero 1)
* **Equipo Oficial (Orden de Lista):**
  1. **Benavides Manzo Fernando** (Ing. 1: Calidad, Nulos y Outliers)
  2. **López Ordorica Ángel Alejandro** (Ing. 2: Transformación e Integración)
  3. **Velázquez Gallardo Andrea Tairi** (Ing. 3: Validación, Resiliencia y Base de Datos)

---

## 1. RESUMEN EJECUTIVO DEL PERFILADO DE CALIDAD

Como Ingeniero 1 responsable del diagnóstico objetivo de calidad, imputación estadística y tratamiento de valores extremos, se implementó el módulo `ing1_calidad_outliers.py` para procesar y auditar los tres conjuntos de datos suministrados para el **Grupo 2 (Fintech & Transacciones Bancarias)**:
- `catalogo_cuentas.csv` (Catálogo Maestro del Core Bancario)
- `transacciones_atm.csv` (Operaciones Físicas en Cajeros Automáticos)
- `transferencias_digitales.json` (Operaciones Digitales SPEI / App Móvil / Web Banking)

El perfilado inicial reveló deficiencias críticas que comprometían la integridad operativa y analítica del sistema: **20 transacciones duplicadas por reintentos de red de cajero, 87 valores faltantes (nulos y vacíos), 184 inconsistencias tipográficas, 61 cuentas huérfanas, 46 CLABEs truncas, 30 correos corruptos y valores atípicos multimillonarios de hasta \$3.5M en ATM y \$80.7M en SPEI (MXN)**.

A continuación se presenta el balance cuantitativo integral Antes vs. Después del preprocesamiento.

---

## 2. TABLA COMPARATIVA GLOBAL DE VOLUMEN Y UNICIDAD

| Dimensión de Auditoría | Dataset ATM (Crudo) | Dataset ATM (Post-Limpieza) | Dataset SPEI (Crudo) | Dataset SPEI (Post-Limpieza) | Total Consolidado |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Registros Totales** | 620 | **600** | 600 | **600** | **1,200 únicos** |
| **Duplicados Exactos** | 20 | **0** | 0 | **0** | **0 duplicados** |
| **Duplicados por Llave Primaria** | 20 | **0** | 0 | **0** | **0 duplicados** |
| **Política de Deduplicación** | — | `keep='first'` | — | Sin duplicados | — |
| **Registros Válidos Conformes** | — | **568** (en prod.) | — | **504** (en prod.) | **1,072 limpios** |
| **Registros en Cuarentena** | — | **32** (huérfanos) | — | **96** (corruptos) | **128 aislados** |

---

## 3. TABLA COMPARATIVA DE COMPLETITUD Y MANEJO DE FALTANTES

| Campo / Columna | Nulos Iniciales | % Inicial | Clasificación Estadística | Técnica de Imputación Aplicada | Nulos Finales | % Final |
| :--- | :---: | :---: | :---: | :--- | :---: | :---: |
| **ATM: `monto_mxn`** | 27 | 4.35% | **MAR** (*Missing at Random*) | Mediana segmentada por `tipo_operacion` (\$2,000 / \$4,000) | **0** | **0.00%** |
| **ATM: `id_cajero`** | 44 | 7.10% | **MCAR** (*Missing Completely at Random*) | Etiqueta controlada `'ATM-NO-IDENTIFICADO'` | **0** | **0.00%** |
| **ATM: `comision_aplicada`** | 0 | 0.00% | Conforme | Preservación de valores reales (`$0.0`, `$15.0`, `$25.0`, `$34.8`) | **0** | **0.00%** |
| **SPEI: `notification_email` (nulos)** | 8 | 1.33% | **MCAR** (*Missing Completely at Random*) | Correo controlado institucional `'anonimo@banco.com.mx'` | **0** | **0.00%** |
| **SPEI: `notification_email` (vacíos)** | 8 | 1.33% | **MCAR** (*Missing Completely at Random*) | Correo controlado institucional `'anonimo@banco.com.mx'` | **0** | **0.00%** |
| **SPEI: `notification_email` (corruptos)** | 30 | 5.00% | No Aplica (Corrupción) | **NO se imputa**; se preserva para desvío a Cuarentena en Fase 6 | **30** | Aislados |

> [!NOTE]
> **Preservación de Registros con `monto_mxn == 0.0`:**
> Se detectaron exactamente **101 transacciones en ATM con `monto_mxn == 0.0`**. Una auditoría cruzada contra la columna `tipo_operacion` demostró que el 100% de estos registros corresponden exclusivamente a `CONSULTA_SALDO`. En ingeniería financiera, consultar un saldo en cajero no transfiere efectivo; por ende, \$0.0 es un valor monetario legítimo y conforme que no debe ser eliminado ni imputado.

---

## 4. JUSTIFICACIÓN MATEMÁTICA: MEDIANA VS. MEDIA ANTE ASIMETRÍA EXTREMA

Una de las decisiones más críticas defendidas en este proyecto es el rechazo categórico de la **media aritmética** en favor de la **mediana estadística** para la imputación de valores faltantes. A continuación se presentan las cifras reales calculadas sobre los datos del Grupo 2:

### Métricas de Dispersión y Asimetría en ATM:
- **Media Aritmética ($\mu$):** $\$9,316.53\text{ MXN}$
- **Desviación Estándar ($\sigma$):** $\$143,637.60\text{ MXN}$
- **Coeficiente de Variación ($CV = \frac{\sigma}{\mu}$):** $1,541.74\%$ (Varianza extrema)
- **Mediana Real ($Q_2$):** $\$2,000.00\text{ MXN}$ (Retiros) | $\$4,000.00\text{ MXN}$ (Depósitos y Pagos)

### Demostración Matemática:
La media aritmética está definida por:
$$\mu = \frac{1}{n} \sum_{i=1}^{n} x_i$$
Basta con que exista un solo valor anómalo masivo, tal como el retiro no autorizado de **\$3,500,000.00 MXN en `ATM-TX-00416`**, para que el numerador se infle artificialmente. Si se hubiera imputado la media (\$9,316.53) a los 16 retiros con importe faltante, se habrían inyectado más de **\$149,000.00 MXN ficticios** al sistema contable del banco, provocando descuadres en la caja física de las sucursales.

En contraste, la **mediana** es un estimador estadístico robusto de orden L con un **punto de ruptura (*breakdown point*) del 50%**, lo que significa que requiere que más del 50% de los datos estén completamente corrompidos para sesgarse. Al utilizar la mediana por tipo de operación:
$$\text{Imputación}_{\text{Retiro}} = \$2,000.00, \quad \text{Imputación}_{\text{Depósito}} = \$4,000.00, \quad \text{Imputación}_{\text{Pago}} = \$4,000.00$$
se asegura que el valor imputado coincide exactamente con la moda y la mediana operativa de los clientes en Colima.

---

## 5. TABLA COMPARATIVA DE OUTLIERS: WINSORIZING / CAPPING VS. ELIMINACIÓN

| Métrica Estadística | Canal ATM (Crudo) | Canal ATM (Post-Capping) | Canal SPEI (Crudo MXN @ \$19.50) | Canal SPEI (Post-Capping MXN) |
| :--- | :---: | :---: | :---: | :---: |
| **Percentil 25 ($Q_1$)** | $\$1,000.00$ | $\$1,000.00$ | $\$12,354.45$ | $\$12,354.45$ |
| **Mediana ($Q_2$)** | $\$3,000.00$ | $\$3,000.00$ | $\$20,689.21$ | $\$20,689.21$ |
| **Percentil 75 ($Q_3$)** | $\$7,000.00$ | $\$7,000.00$ | $\$34,624.82$ | $\$34,624.82$ |
| **Rango Intercuartílico ($IQR$)** | $\$6,000.00$ | $\$6,000.00$ | $\$22,270.37$ | $\$22,270.37$ |
| **Umbral Inferior ($L_{\text{inf}}$)** | $\$0.00$ | $\$0.00$ | $\$0.00$ | $\$0.00$ |
| **Umbral Superior ($L_{\text{sup}} = Q_3 + 1.5 \times IQR$)** | $\$16,000.00$ | $\$16,000.00$ | $\$68,030.38$ | $\$68,030.38$ |
| **Importe Máximo Registrado** | **\$3,500,000.00** | **\$16,000.00** (Capped) | **\$80,757,319.50** | **\$68,030.38** (Capped) |
| **Transacciones Capped** | 0 | **1 registro** (`ATM-TX-00416`) | 0 | **138 transferencias** |
| **Filas Destruidas / Borradas** | 0 | **0 filas borradas** | 0 | **0 filas borradas** |

### Fundamentación Técnica del Winsorizing:
La eliminación ciega de registros atípicos (*row deletion*) es una mala práctica en ingeniería financiera porque borra evidencia transaccional legítima. Mediante la técnica de **Winsorizing / Capping**, el registro se mantiene en la base de datos con su identificador, fecha y cuenta; el importe se trunca exactamente a $L_{\text{sup}}$ para estabilizar la varianza, se almacena el monto original en la columna de auditoría `monto_original_pre_capping` y se activa la bandera booleana `fue_capped_iqr = True`.

---

## 6. COMPARATIVA DE DISTRIBUCIÓN ESTADÍSTICA (ANTES VS. DESPUÉS)

A continuación se resumen los estadísticos descriptivos antes y después de la limpieza:

```text
================================================================================================
ESTADÍSTICO                      ATM CRUDO           ATM LIMPIO          SPEI CRUDO (MXN)    SPEI LIMPIO (MXN)
================================================================================================
Conteo Válido                    593                 600                 600                 600
Media (MXN)                      $9,316.53           $3,513.33           $98,261.81          $24,082.91
Desviación Estándar (MXN)        $143,637.60         $3,299.78           $533,615.80         $18,485.45
Mínimo                           $0.00               $0.00               $73.62              $73.62
25% (Q1)                         $500.00             $1,000.00           $12,354.45          $12,354.45
50% (Mediana)                    $2,000.00           $3,000.00           $20,689.21          $20,689.21
75% (Q3)                         $7,000.00           $5,000.00           $34,624.82          $34,624.82
Máximo                           $3,500,000.00       $16,000.00          $80,757,319.50      $68,030.38
Varianza Reducida                —                   -99.95%             —                   -99.88%
================================================================================================
```

---

## 7. CONCLUSIÓN DE INGENIERÍA 1

La intervención técnica sobre los datos crudos permitió transformar un conjunto altamente contaminado y asimétrico en un flujo estandarizado y listo para la fase de integración y validación. Se neutralizaron las distorsiones estadísticas sin destruir transacciones bancarias válidas, garantizando la trazabilidad auditable y proporcionando a los Ingenieros 2 y 3 una base sólida y confiable.

```text
                                  ATENTAMENTE,

                          ___________________________
                           BENAVIDES MANZO, FERNANDO
                                  Ingeniero 1
                    Especialista en Calidad, Nulos y Outliers
```
