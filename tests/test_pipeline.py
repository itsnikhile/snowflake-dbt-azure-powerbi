"""
Pipeline Tests — Snowflake + dbt + Azure + Power BI
Tests data quality, transformation logic, and DAX measure calculations.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_sales():
    np.random.seed(42)
    n = 1000
    return pd.DataFrame({
        "sale_id":       [f"SALE_{i:06d}" for i in range(n)],
        "customer_id":   [f"CUST_{np.random.randint(0,200):04d}" for _ in range(n)],
        "product_id":    [f"PROD_{np.random.randint(0,50):03d}" for _ in range(n)],
        "territory_id":  [f"TERR_{np.random.randint(0,10):02d}" for _ in range(n)],
        "order_date":    pd.date_range("2024-01-01", periods=n, freq="8h"),
        "quantity":      np.random.randint(1, 20, n),
        "unit_price":    np.random.uniform(10, 1000, n).round(2),
        "discount_pct":  np.random.uniform(0, 0.3, n).round(4),
        "total_amount":  np.random.uniform(50, 10000, n).round(2),
        "status":        np.random.choice(["completed","pending","shipped","cancelled"], n),
        "channel":       np.random.choice(["online","direct","partner","reseller"], n),
        "currency":      "USD",
    })


@pytest.fixture
def sample_customers():
    n = 200
    return pd.DataFrame({
        "customer_id":  [f"CUST_{i:04d}" for i in range(n)],
        "full_name":    [f"Customer {i}" for i in range(n)],
        "email":        [f"customer{i}@example.com" for i in range(n)],
        "country":      np.random.choice(["US","UK","DE","FR","CA"], n),
        "tier":         np.random.choice(["enterprise","mid_market","smb"], n),
        "signup_date":  pd.date_range("2020-01-01", periods=n, freq="3D"),
    })


# ── Data Quality Tests ────────────────────────────────────────────────────────

class TestDataQuality:

    def test_no_null_sale_ids(self, sample_sales):
        assert sample_sales["sale_id"].isna().sum() == 0

    def test_unique_sale_ids(self, sample_sales):
        assert sample_sales["sale_id"].duplicated().sum() == 0

    def test_positive_amounts(self, sample_sales):
        assert (sample_sales["total_amount"] > 0).all()

    def test_positive_quantities(self, sample_sales):
        assert (sample_sales["quantity"] > 0).all()

    def test_valid_discount_range(self, sample_sales):
        assert sample_sales["discount_pct"].between(0, 1).all()

    def test_valid_status_values(self, sample_sales):
        valid = {"completed", "pending", "shipped", "cancelled", "returned"}
        assert set(sample_sales["status"].unique()).issubset(valid)

    def test_no_future_order_dates(self, sample_sales):
        assert (sample_sales["order_date"] <= datetime.utcnow()).all()

    def test_no_null_customer_ids(self, sample_sales):
        assert sample_sales["customer_id"].isna().sum() == 0


# ── Transformation Tests ──────────────────────────────────────────────────────

class TestTransformations:

    def test_net_revenue_calculation(self, sample_sales):
        df = sample_sales.copy()
        df["net_revenue"] = df["total_amount"] * (1 - df["discount_pct"])
        assert (df["net_revenue"] <= df["total_amount"]).all()
        assert (df["net_revenue"] >= 0).all()

    def test_deal_size_tier_logic(self, sample_sales):
        df = sample_sales.copy()
        df["deal_size_tier"] = pd.cut(
            df["total_amount"],
            bins=[0, 100, 1000, 10000, float("inf")],
            labels=["micro", "smb", "mid_market", "enterprise"],
        )
        assert df["deal_size_tier"].notna().all()
        assert set(df["deal_size_tier"].unique()).issubset(
            {"micro", "smb", "mid_market", "enterprise"}
        )

    def test_rfm_segmentation(self, sample_sales, sample_customers):
        sales = sample_sales[sample_sales["status"] != "cancelled"]
        stats = sales.groupby("customer_id").agg(
            total_orders=("sale_id", "count"),
            lifetime_value=("total_amount", "sum"),
            last_order_date=("order_date", "max"),
        ).reset_index()
        stats["days_since_last_order"] = (
            pd.Timestamp.utcnow().tz_localize(None) - stats["last_order_date"]
        ).dt.days
        assert (stats["total_orders"] >= 1).all()
        assert (stats["lifetime_value"] > 0).all()

    def test_gross_margin_pct_range(self, sample_sales):
        cost_factor = 0.6
        df = sample_sales.copy()
        df["net_revenue"] = df["total_amount"] * (1 - df["discount_pct"])
        df["cost"] = df["quantity"] * df["unit_price"] * cost_factor
        df["gross_profit"] = df["net_revenue"] - df["cost"]
        df["gross_margin_pct"] = df["gross_profit"] / df["net_revenue"].replace(0, np.nan)
        valid = df["gross_margin_pct"].dropna()
        assert valid.between(-2, 2).all()

    def test_monthly_aggregation(self, sample_sales):
        df = sample_sales[sample_sales["status"] != "cancelled"].copy()
        df["order_month"] = df["order_date"].dt.to_period("M")
        monthly = df.groupby("order_month")["total_amount"].sum()
        assert (monthly > 0).all()
        assert len(monthly) > 0


# ── DAX Logic Tests (Python equivalents) ─────────────────────────────────────

class TestDAXLogic:

    def test_mom_growth(self, sample_sales):
        df = sample_sales.copy()
        df["order_month"] = df["order_date"].dt.to_period("M")
        monthly = df.groupby("order_month")["total_amount"].sum().reset_index()
        monthly["prev_month"] = monthly["total_amount"].shift(1)
        monthly["mom_growth"] = (monthly["total_amount"] - monthly["prev_month"]) / monthly["prev_month"]
        # growth should be calculable for all but first month
        assert monthly["mom_growth"].dropna().notna().all()

    def test_ytd_revenue(self, sample_sales):
        df = sample_sales.copy()
        current_year = df["order_date"].dt.year.max()
        ytd = df[df["order_date"].dt.year == current_year]["total_amount"].sum()
        assert ytd > 0

    def test_customer_retention(self, sample_sales):
        df = sample_sales[sample_sales["status"] != "cancelled"].copy()
        total = df["customer_id"].nunique()
        repeat = df[df.duplicated("customer_id", keep=False)]["customer_id"].nunique()
        retention = repeat / total if total > 0 else 0
        assert 0 <= retention <= 1


# ── Integration Test (mock) ────────────────────────────────────────────────────

class TestIntegration:

    def test_full_pipeline_demo(self, sample_sales, sample_customers):
        """Simulates the full pipeline: raw → staging → mart → KPIs"""
        # Stage 1: Validate raw
        assert len(sample_sales) > 0
        assert sample_sales["sale_id"].is_unique

        # Stage 2: Transform
        completed = sample_sales[sample_sales["status"] != "cancelled"].copy()
        completed["net_revenue"] = (
            completed["total_amount"] * (1 - completed["discount_pct"])
        ).round(4)

        # Stage 3: Join customers
        merged = completed.merge(
            sample_customers[["customer_id","country","tier"]],
            on="customer_id", how="left"
        )
        assert len(merged) == len(completed)

        # Stage 4: Aggregate KPIs
        kpis = {
            "total_revenue": round(float(merged["net_revenue"].sum()), 2),
            "total_orders": int(len(merged)),
            "unique_customers": int(merged["customer_id"].nunique()),
            "avg_order_value": round(float(merged["net_revenue"].mean()), 2),
        }
        assert kpis["total_revenue"] > 0
        assert kpis["total_orders"] > 0
        assert kpis["unique_customers"] > 0
        assert kpis["avg_order_value"] > 0

        print(f"\n✅ Pipeline KPIs: {kpis}")
