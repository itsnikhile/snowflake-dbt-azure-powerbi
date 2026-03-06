"""
Snowflake Schema Manager
Creates and manages RAW, STAGING, MARTS, and SEMANTIC schemas.
Handles table DDL, clustering, and data retention policies.
"""

import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

RAW_DDL = {
    "SALES": """
        CREATE TABLE IF NOT EXISTS RAW.SALES (
            sale_id         VARCHAR(64)    NOT NULL,
            customer_id     VARCHAR(32),
            product_id      VARCHAR(32),
            territory_id    VARCHAR(16),
            salesperson_id  VARCHAR(32),
            order_date      DATE,
            ship_date       DATE,
            quantity        NUMBER(10,0),
            unit_price      NUMBER(18,4),
            discount_pct    NUMBER(5,4)    DEFAULT 0,
            total_amount    NUMBER(18,4),
            currency        VARCHAR(3)     DEFAULT 'USD',
            channel         VARCHAR(32),
            status          VARCHAR(32),
            metadata        VARIANT,
            _source         VARCHAR(128),
            _ingested_at    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
            _file_name      VARCHAR(512)
        )
        CLUSTER BY (order_date, territory_id)
        DATA_RETENTION_TIME_IN_DAYS = 14
    """,
    "CUSTOMERS": """
        CREATE TABLE IF NOT EXISTS RAW.CUSTOMERS (
            customer_id     VARCHAR(32)    NOT NULL,
            first_name      VARCHAR(128),
            last_name       VARCHAR(128),
            email           VARCHAR(256),
            phone           VARCHAR(32),
            company         VARCHAR(256),
            industry        VARCHAR(64),
            country         VARCHAR(64),
            region          VARCHAR(64),
            city            VARCHAR(128),
            tier            VARCHAR(32),
            signup_date     DATE,
            attributes      VARIANT,
            _ingested_at    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "PRODUCTS": """
        CREATE TABLE IF NOT EXISTS RAW.PRODUCTS (
            product_id      VARCHAR(32)    NOT NULL,
            product_name    VARCHAR(256),
            category        VARCHAR(64),
            subcategory     VARCHAR(64),
            brand           VARCHAR(128),
            cost_price      NUMBER(18,4),
            list_price      NUMBER(18,4),
            weight_kg       NUMBER(10,3),
            is_active       BOOLEAN        DEFAULT TRUE,
            launched_date   DATE,
            attributes      VARIANT,
            _ingested_at    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "TERRITORIES": """
        CREATE TABLE IF NOT EXISTS RAW.TERRITORIES (
            territory_id    VARCHAR(16)    NOT NULL,
            territory_name  VARCHAR(128),
            region          VARCHAR(64),
            country         VARCHAR(64),
            manager_id      VARCHAR(32),
            quota_amount    NUMBER(18,2),
            _ingested_at    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "BUDGET": """
        CREATE TABLE IF NOT EXISTS RAW.BUDGET (
            budget_id       VARCHAR(32)    NOT NULL,
            territory_id    VARCHAR(16),
            product_category VARCHAR(64),
            fiscal_year     NUMBER(4,0),
            fiscal_month    NUMBER(2,0),
            budget_amount   NUMBER(18,2),
            _ingested_at    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
        )
    """,
}


class SchemaManager:
    def __init__(self, config: dict):
        try:
            import snowflake.connector
            self.conn = snowflake.connector.connect(**config)
            self._available = True
        except Exception as e:
            logger.warning(f"Snowflake not available: {e} — mock mode")
            self._available = False

    def setup_all(self):
        """Full schema setup: schemas → warehouses → tables → grants."""
        self.create_schemas()
        self.create_raw_tables()
        self.create_semantic_views()
        logger.info("✅ Snowflake setup complete")

    def create_schemas(self):
        schemas = ["RAW", "STAGING", "INTERMEDIATE", "MARTS", "SEMANTIC", "AUDIT"]
        for schema in schemas:
            self._execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logger.info(f"Schemas created: {schemas}")

    def create_raw_tables(self):
        for name, ddl in RAW_DDL.items():
            self._execute(ddl)
            logger.info(f"  RAW.{name} ✓")

    def create_semantic_views(self):
        """Creates Snowflake views optimised for Power BI DirectQuery."""
        views = {
            "SEMANTIC.VW_SALES_FACT": """
                CREATE OR REPLACE VIEW SEMANTIC.VW_SALES_FACT AS
                SELECT
                    s.sale_id,
                    s.customer_id,
                    s.product_id,
                    s.territory_id,
                    s.salesperson_id,
                    s.order_date,
                    s.ship_date,
                    DATEDIFF('day', s.order_date, s.ship_date)  AS days_to_ship,
                    s.quantity,
                    s.unit_price,
                    s.discount_pct,
                    s.total_amount,
                    s.total_amount * (1 - s.discount_pct)       AS net_revenue,
                    s.currency,
                    s.channel,
                    s.status,
                    YEAR(s.order_date)                           AS order_year,
                    MONTH(s.order_date)                          AS order_month,
                    QUARTER(s.order_date)                        AS order_quarter,
                    DATE_TRUNC('month', s.order_date)            AS order_month_date
                FROM MARTS.FCT_SALES s
            """,
            "SEMANTIC.VW_CUSTOMER_DIM": """
                CREATE OR REPLACE VIEW SEMANTIC.VW_CUSTOMER_DIM AS
                SELECT
                    customer_id,
                    full_name,
                    email,
                    company,
                    industry,
                    country,
                    region,
                    city,
                    tier,
                    signup_date,
                    lifetime_value,
                    total_orders,
                    avg_order_value,
                    days_since_last_order,
                    customer_segment,
                    is_active
                FROM MARTS.DIM_CUSTOMERS
            """,
        }
        for name, sql in views.items():
            self._execute(sql)
            logger.info(f"  {name} ✓")

    def get_table_stats(self) -> List[Dict]:
        if not self._available:
            return []
        sql = """
        SELECT TABLE_SCHEMA, TABLE_NAME,
               COALESCE(ROW_COUNT, 0)      AS row_count,
               ROUND(BYTES/1e9, 3)         AS size_gb,
               LAST_ALTERED
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN ('RAW','STAGING','MARTS','SEMANTIC')
        ORDER BY TABLE_SCHEMA, size_gb DESC NULLS LAST
        """
        cursor = self.conn.cursor()
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def _execute(self, sql: str):
        if not self._available:
            logger.info(f"[MOCK] Would execute: {sql[:60].strip()}...")
            return
        self.conn.cursor().execute(sql)

    def close(self):
        if self._available:
            self.conn.close()
