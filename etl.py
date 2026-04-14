import logging
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TABLES = [
    "orders",
    "order_items",
    "customers",
    "products",
    "brands",
    "categories",
    "inventory",
    "promotions",
    "reviews",
    "suppliers",
    "warehouses",
]

# Columnas de fecha por tabla — se parsean automaticamente en el extract
DATE_COLUMNS = {
    "orders": ["order_date"],
    "customers": ["birth_date", "registration_date", "last_login"],
    "products": ["created_at", "updated_at"],
    "inventory": ["last_restock_date"],
    "promotions": ["start_date", "end_date"],
    "reviews": ["created_at"],
}

# Columnas criticas por tabla — si tienen nulos, esas filas se descartan
CRITICAL_COLUMNS = {
    "orders": ["order_id", "customer_id", "total_amount", "order_date"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    "customers": ["customer_id", "email"],
    "products": ["product_id", "sku", "price"],
    "reviews": ["review_id", "product_id", "customer_id", "rating"],
}


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def extract() -> dict[str, pd.DataFrame]:
    """Lee todos los CSVs raw y retorna un dict {nombre: DataFrame}."""
    dataframes: dict[str, pd.DataFrame] = {}

    for table in TABLES:
        path = RAW_DIR / f"ecommerce_{table}.csv"
        if not path.exists():
            logger.warning("Archivo no encontrado: %s", path)
            continue

        parse_dates = DATE_COLUMNS.get(table, [])
        df = pd.read_csv(path, parse_dates=parse_dates)
        dataframes[table] = df
        logger.info("Extraido %-15s %d filas, %d columnas", table, len(df), len(df.columns))

    logger.info("Extract completo: %d tablas cargadas", len(dataframes))
    return dataframes


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------
def _drop_critical_nulls(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Elimina filas con nulos en columnas criticas de la tabla."""
    cols = CRITICAL_COLUMNS.get(table)
    if cols is None:
        return df

    before = len(df)
    df = df.dropna(subset=cols)
    dropped = before - len(df)

    if dropped > 0:
        logger.warning("%s: %d filas eliminadas por nulos en columnas criticas %s", table, dropped, cols)
    else:
        logger.info("%s: integridad de columnas criticas OK", table)

    return df


def _drop_duplicate_ids(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Elimina filas con IDs duplicados (conserva la primera)."""
    id_col = df.columns[0]
    before = len(df)
    df = df.drop_duplicates(subset=[id_col], keep="first")
    dropped = before - len(df)

    if dropped > 0:
        logger.warning("%s: %d filas duplicadas en '%s' eliminadas", table, dropped, id_col)

    return df


def _transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    # promotion_id: nulo significa sin promocion -> -1, convertir a entero
    df["promotion_id"] = df["promotion_id"].fillna(-1).astype(int)
    return df


def _transform_categories(df: pd.DataFrame) -> pd.DataFrame:
    # parent_category_id: nulo significa categoria raiz -> -1, convertir a entero
    df["parent_category_id"] = df["parent_category_id"].fillna(-1).astype(int)
    return df


def _transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    # Normalizar emails a minusculas
    df["email"] = df["email"].str.lower().str.strip()
    return df


TABLE_TRANSFORMS = {
    "orders": _transform_orders,
    "categories": _transform_categories,
    "customers": _transform_customers,
}


def transform(dataframes: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Aplica transformaciones de limpieza a cada tabla."""
    cleaned: dict[str, pd.DataFrame] = {}

    for table, df in dataframes.items():
        df = _drop_duplicate_ids(df, table)
        df = _drop_critical_nulls(df, table)
        
        transform_fn = TABLE_TRANSFORMS.get(table)
        if transform_fn:
            df = transform_fn(df)
            logger.info("%s: transformacion especifica aplicada", table)

        cleaned[table] = df

    logger.info("Transform completo: %d tablas procesadas", len(cleaned))
    return cleaned


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def load(dataframes: dict[str, pd.DataFrame]) -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    for table, df in dataframes.items():
        # Opcion CSV:
        # path = CLEAN_DIR / f"cleaned_ecommerce_{table}.csv"
        # df.to_csv(path, index=False)

        # Opcion Parquet (mas compacto, rapido y preserva tipos):
        path = CLEAN_DIR / f"cleaned_ecommerce_{table}.parquet"
        df.to_parquet(path, index=False)
        logger.info("Guardado %-15s -> %s (%d filas)", table, path.name, len(df))

    logger.info("Load completo: %d archivos guardados en %s", len(dataframes), CLEAN_DIR)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    raw = extract()
    clean = transform(raw)
    load(clean)

