# Jesus Jimenez's Project

# E-commerce ETL Pipeline

Pipeline ETL en Python que extrae, limpia y analiza datos de un e-commerce a partir de 11 tablas CSV (orders, customers, products, reviews, etc.).

## Qué hace el proyecto

1. **Extract**: Lee 11 archivos CSV crudos desde `data/raw/`.
2. **Transform**: Limpia los datos — elimina duplicados por ID, descarta filas con nulos en columnas críticas, normaliza emails, parsea fechas y convierte IDs opcionales (como `promotion_id`) a enteros.
3. **Load**: Guarda las tablas limpias en formato Parquet en `data/clean/`.
4. **Analysis**: Responde 3 preguntas de negocio a partir de los datos limpios y exporta los resultados a `output/`.

### Preguntas de negocio

| # | Pregunta | Archivo de salida |
|---|----------|-------------------|
| 1 | Top 5 clientes que más gastaron | `top_5_clientes.parquet` |
| 2 | Ranking de productos más vendidos por unidades | `ranking_productos.parquet` |
| 3 | Evolución de ventas mes a mes (ingresos, órdenes, ticket promedio) | `ventas_mensuales.parquet` |

## Cómo correrlo

**Requisitos**: Python 3.10+ y las dependencias:

```bash
pip3 install pandas pyarrow
```

**Ejecución** (en orden):

```bash
# Paso 1 — ETL: extrae, transforma y carga los datos limpios
python3 etl.py

# Paso 2 — Análisis: genera los reportes de negocio
python3 analysis.py
```

El paso 2 depende del paso 1 — necesita que existan los archivos limpios en `data/clean/`.

## Estructura del proyecto

```
├── etl.py              # Pipeline ETL (extract → transform → load)
├── analysis.py         # Consultas analíticas sobre datos limpios
├── data/
│   ├── raw/            # 11 CSVs originales (ecommerce_*.csv)
│   └── clean/          # Tablas limpias en Parquet (generadas por etl.py)
└── output/             # Resultados de las preguntas de negocio (generados por analysis.py)
```

## Decisiones técnicas

- **Parquet en lugar de CSV** para la capa limpia y los outputs. Parquet comprime mejor, es más rápido de leer/escribir y preserva los tipos de datos (fechas, enteros) sin necesidad de re-parsear. El código incluye las líneas de CSV comentadas para poder alternar fácilmente.

- **Columnas críticas por tabla** (`CRITICAL_COLUMNS`): en lugar de eliminar nulos en todas las columnas, se definen columnas críticas por tabla. Si una fila tiene nulos en esas columnas, se descarta; el resto de nulos se toleran. Esto evita perder datos innecesariamente.

- **Transformaciones específicas registradas en un diccionario** (`TABLE_TRANSFORMS`): cada tabla que necesita lógica especial (normalizar emails, convertir IDs nulos a -1) tiene su función. Las tablas sin transformación especial simplemente pasan por la limpieza genérica (dedup + nulos).

- **IDs opcionales como -1** en vez de NaN: `promotion_id` y `parent_category_id` usan -1 para representar "sin valor" y se convierten a entero. Esto evita que pandas los trate como float64 (comportamiento por defecto cuando una columna int tiene NaN).

- **Scripts independientes sin módulos compartidos**: al ser un proyecto pequeño con dos scripts, se priorizó la simplicidad sobre la reutilización. Cada script se configura y ejecuta de forma autónoma.
