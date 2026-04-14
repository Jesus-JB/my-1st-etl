import logging
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUT_DIR = BASE_DIR / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_clean_data() -> dict[str, pd.DataFrame]:
    """Carga los DataFrames limpios necesarios para el analisis."""
    # Opcion CSV:
    # orders = pd.read_csv(CLEAN_DIR / "cleaned_ecommerce_orders.csv", parse_dates=["order_date"])
    # order_items = pd.read_csv(CLEAN_DIR / "cleaned_ecommerce_order_items.csv")
    # customers = pd.read_csv(CLEAN_DIR / "cleaned_ecommerce_customers.csv")
    # products = pd.read_csv(CLEAN_DIR / "cleaned_ecommerce_products.csv")

    # Opcion Parquet:
    orders = pd.read_parquet(CLEAN_DIR / "cleaned_ecommerce_orders.parquet")
    order_items = pd.read_parquet(CLEAN_DIR / "cleaned_ecommerce_order_items.parquet")
    customers = pd.read_parquet(CLEAN_DIR / "cleaned_ecommerce_customers.parquet")
    products = pd.read_parquet(CLEAN_DIR / "cleaned_ecommerce_products.parquet")

    return {
        "orders": orders,
        "order_items": order_items,
        "customers": customers,
        "products": products,
    }


# ---------------------------------------------------------------------------
# Pregunta 1: Top 5 clientes que mas gastaron
# ---------------------------------------------------------------------------
def top_5_customers(orders: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa orders por customer_id, suma total_amount,
    y hace join con customers para obtener los nombres.
    """
    customer_spending = (
        orders
        .groupby("customer_id")
        .agg(
            total_gastado=("total_amount", "sum"),
            cantidad_ordenes=("order_id", "count"),
        )
        .reset_index()
    )

    customer_spending = customer_spending.merge(
        customers[["customer_id", "first_name", "last_name"]],
        on="customer_id",
    )

    top_5 = (
        customer_spending
        .sort_values("total_gastado", ascending=False)
        .head(5)
        [["customer_id", "first_name", "last_name", "total_gastado", "cantidad_ordenes"]]
    )

    return top_5


# ---------------------------------------------------------------------------
# Pregunta 2: Producto mas vendido (por cantidad)
# ---------------------------------------------------------------------------
def most_sold_products(order_items: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa order_items por product_id, suma quantity,
    y hace join con products para obtener los nombres.
    """
    product_sales = (
        order_items
        .groupby("product_id")
        .agg(
            unidades_vendidas=("quantity", "sum"),
            ingresos_totales=("subtotal", "sum"),
        )
        .reset_index()
    )

    product_sales = product_sales.merge(
        products[["product_id", "product_name"]],
        on="product_id",
    )

    product_sales = (
        product_sales
        .sort_values("unidades_vendidas", ascending=False)
        [["product_id", "product_name", "unidades_vendidas", "ingresos_totales"]]
    )

    return product_sales


# ---------------------------------------------------------------------------
# Pregunta 3: Evolucion de ventas mes a mes
# ---------------------------------------------------------------------------
def monthly_sales(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte order_date a periodo mensual,
    agrupa por mes y suma ventas.
    """
    orders["order_month"] = orders["order_date"].dt.to_period("M")

    monthly = (
        orders
        .groupby("order_month")
        .agg(
            ingresos_totales=("total_amount", "sum"),
            cantidad_ordenes=("order_id", "count"),
        )
        .reset_index()
    )

    monthly["ticket_promedio"] = monthly["ingresos_totales"] / monthly["cantidad_ordenes"]

    monthly = monthly.sort_values("order_month")

    return monthly


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    data = load_clean_data()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Pregunta 1 ---
    logger.info("Pregunta 1: Top 5 clientes que mas gastaron")
    top5 = top_5_customers(data["orders"], data["customers"])
    print("\n" + "=" * 60)
    print("TOP 5 CLIENTES QUE MAS GASTARON")
    print("=" * 60)
    print(top5.to_string(index=False))
    # top5.to_csv(OUTPUT_DIR / "top_5_clientes.csv", index=False)
    top5.to_parquet(OUTPUT_DIR / "top_5_clientes.parquet", index=False)

    # --- Pregunta 2 ---
    logger.info("Pregunta 2: Producto mas vendido por cantidad")
    products_ranking = most_sold_products(data["order_items"], data["products"])
    print("\n" + "=" * 60)
    print("RANKING DE PRODUCTOS POR UNIDADES VENDIDAS")
    print("=" * 60)
    print(products_ranking.to_string(index=False))
    # products_ranking.to_csv(OUTPUT_DIR / "ranking_productos.csv", index=False)
    products_ranking.to_parquet(OUTPUT_DIR / "ranking_productos.parquet", index=False)

    # --- Pregunta 3 ---
    logger.info("Pregunta 3: Evolucion de ventas mes a mes")
    monthly = monthly_sales(data["orders"])
    print("\n" + "=" * 60)
    print("VENTAS MES A MES")
    print("=" * 60)
    print(monthly.to_string(index=False))
    # monthly.to_csv(OUTPUT_DIR / "ventas_mensuales.csv", index=False)
    monthly.to_parquet(OUTPUT_DIR / "ventas_mensuales.parquet", index=False)

    logger.info("Resultados guardados en %s", OUTPUT_DIR)
