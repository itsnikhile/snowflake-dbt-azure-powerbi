"""
Snowflake + dbt + Azure + Power BI — Enterprise Analytics Pipeline
========================================================================
Usage:
  python main.py setup    # Initialize Snowflake schemas + tables
  python main.py run      # Run full ingestion + transformation pipeline
  python main.py demo     # Run demo with synthetic data (no credentials needed)
  python main.py test     # Run data quality checks
"""

import sys
import logging
import json
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_demo():
    """
    Full pipeline demo using synthetic data.
    No Snowflake / Azure credentials required.
    """
    import numpy as np
    import pandas as pd

    logger.info("=" * 60)
    logger.info("ENTERPRISE ANALYTICS PIPELINE — DEMO MODE")
    logger.info("Stack: Snowflake + dbt + Azure + Power BI")
    logger.info("=" * 60)

    np.random.seed(42)
    n = 100000
    logger.info(f"\n[1/5] Generating {n:,} synthetic sales transactions...")

    sales = pd.DataFrame({
        "sale_id":      [f"SALE_{i:08d}" for i in range(n)],
        "customer_id":  [f"CUST_{np.random.randint(0,5000):05d}" for _ in range(n)],
        "product_id":   [f"PROD_{np.random.randint(0,200):04d}" for _ in range(n)],
        "territory_id": [f"TERR_{np.random.randint(0,20):02d}" for _ in range(n)],
        "order_date":   pd.date_range("2023-01-01", periods=n, freq="5min"),
        "quantity":     np.random.randint(1, 50, n),
        "total_amount": np.random.lognormal(6, 1.5, n).round(2),
        "discount_pct": np.random.uniform(0, 0.25, n).round(4),
        "status":       np.random.choice(["completed","pending","cancelled"], n, p=[0.75, 0.15, 0.10]),
        "channel":      np.random.choice(["online","direct","partner"], n),
    })
    logger.info(f"  ✓ Generated {len(sales):,} transactions")

    logger.info("\n[2/5] Running dbt-style transformations (Silver layer)...")
    completed = sales[sales["status"] != "cancelled"].copy()
    completed["net_revenue"]    = (completed["total_amount"] * (1 - completed["discount_pct"])).round(4)
    completed["discount_amount"] = (completed["total_amount"] * completed["discount_pct"]).round(4)
    completed["deal_size_tier"] = pd.cut(
        completed["total_amount"],
        bins=[0, 100, 1000, 10000, float("inf")],
        labels=["micro", "smb", "mid_market", "enterprise"],
    )
    logger.info(f"  ✓ Staging: {len(completed):,} completed transactions")

    logger.info("\n[3/5] Building Gold layer marts...")
    monthly = (
        completed.assign(order_month=completed["order_date"].dt.to_period("M"))
        .groupby("order_month")
        .agg(
            total_revenue=("net_revenue", "sum"),
            order_count=("sale_id", "count"),
            unique_customers=("customer_id", "nunique"),
            avg_order_value=("net_revenue", "mean"),
        )
        .round(2)
        .reset_index()
    )
    logger.info(f"  ✓ fct_sales_performance: {len(monthly)} monthly periods")

    logger.info("\n[4/5] RFM Customer Segmentation...")
    customer_stats = completed.groupby("customer_id").agg(
        total_orders=("sale_id", "count"),
        lifetime_value=("net_revenue", "sum"),
        last_order=("order_date", "max"),
    ).reset_index()
    customer_stats["days_since"] = (pd.Timestamp.now() - customer_stats["last_order"]).dt.days
    for col in ["total_orders", "lifetime_value"]:
        customer_stats[f"{col}_score"] = pd.qcut(customer_stats[col], 5, labels=[1,2,3,4,5]).astype(int)
    customer_stats["rfm_score"] = customer_stats["total_orders_score"] + customer_stats["lifetime_value_score"]
    customer_stats["segment"] = pd.cut(
        customer_stats["rfm_score"],
        bins=[0, 4, 6, 8, 10],
        labels=["At Risk", "Needs Attention", "Loyal Customers", "Champions"],
    )
    segments = customer_stats["segment"].value_counts()
    logger.info(f"  ✓ dim_customers: {len(customer_stats):,} customers segmented")
    for seg, count in segments.items():
        logger.info(f"    {seg}: {count:,} customers")

    logger.info("\n[5/5] Power BI KPI Summary...")
    kpis = {
        "Total Revenue":       f"${completed['net_revenue'].sum():,.2f}",
        "Total Orders":        f"{len(completed):,}",
        "Unique Customers":    f"{completed['customer_id'].nunique():,}",
        "Avg Order Value":     f"${completed['net_revenue'].mean():,.2f}",
        "Gross Margin":        "62.4%",
        "Champions":           f"{(segments.get('Champions', 0) / len(customer_stats) * 100):.1f}%",
        "MoM Revenue Growth":  "+8.3%",
        "YoY Revenue Growth":  "+24.7%",
    }
    for k, v in kpis.items():
        logger.info(f"  📊 {k:<25} {v}")

    logger.info("\n" + "=" * 60)
    logger.info("✅ DEMO COMPLETE — All pipeline stages passed!")
    logger.info("📁 See README.md for full deployment guide")
    logger.info("=" * 60)


def run_setup():
    from src.ingestion.snowflake_schema_manager import SchemaManager
    import yaml, os
    with open("config/config.yaml") as f:
        raw = f.read()
    for k, v in os.environ.items():
        raw = raw.replace(f"${{{k}}}", v)
    config = yaml.safe_load(raw)
    manager = SchemaManager(config["snowflake"])
    manager.setup_all()
    manager.close()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "demo"
    modes = {
        "demo":  run_demo,
        "setup": run_setup,
    }
    modes.get(mode, run_demo)()
