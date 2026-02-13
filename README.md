# E-Commerce Analytics Platform (dbt + DuckDB)

## 1. Project Overview
This repository contains a modern data transformation pipeline built with **dbt (data build tool)** and **DuckDB**. The goal is to transform raw e-commerce transactional data into clean, business-ready analytical models using a modular architecture.

The project demonstrates:
* **ELT Best Practices:** Staging → Intermediate → Marts layering.
* **Advanced Modeling:** Incremental models, Snapshots (SCD Type 2), and Custom Macros.
* **Data Quality:** Comprehensive testing (generic & singular) and documentation.
* **Performance:** Optimized materialization strategies.

---

## 2. Architecture & Design

### High-Level Data Pipeline
The pipeline follows a standard dbt lineage structure:

```mermaid
graph LR
    A[Raw Seeds (CSV)] --> B[Staging Layer (Clean/Cast)]
    B --> C[Intermediate Layer (Join/Agg)]
    C --> D[Marts Layer (Business Logic)]
    D --> E[BI / Reporting]
```

### Tech Stack
1. Orchestration & Transformation: dbt Core (v1.8+)
2. Data Warehouse: DuckDB (Serverless, local OLAP database)
3. Language: SQL (DuckDB dialect) & Jinja2
4. Version Control: Git

### Data Modeling Approach
I utilized Dimensional Modeling (Star Schema) for the final marts to ensure compatibility with BI tools (Tableau, Looker, PowerBI) and ease of use for end-users.
**Fact Tables:** fct_orders (Transactional events).
**Dimension Tables:** dim_customers (Descriptive attributes).
**Aggregate Marts:** mart_product_performance, mart_monthly_revenue (Pre-calculated metrics for dashboards).

### Key Design Decisions
1. DuckDB as Warehouse: Chosen for its speed on local development and ability to handle analytical queries on CSVs without infrastructure overhead.
2. Incremental Strategy: fct_orders is configured as incremental. In a real-world scenario, order tables grow indefinitely. This strategy processes only new data (based on order_date), significantly reducing compute costs.
3. SCD Type 2 (Snapshots): Implemented on the products table to track cost changes over time, preserving historical accuracy for revenue reporting.
4. Assumptions:
Cost Calculation: Raw data lacked a 'cost' column in some versions, so a 60% standard cost model was applied in mart_product_performance.
Currency: All monetary values are assumed to be in USD.

## 3. Setup & Installation

### Prerequisites
1. Python 3.8+
2. Git

### Step-by-Step Instructions

1. Clone the Repository
```bash
git clone <your-repo-url>
cd ecommerce-analytics-dbt
```

2. Set up Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install dbt-duckdb
```

3. Load Raw Data (Seeds)
```bash
dbt seed
```
This creates the local DuckDB database file and loads the CSVs.

4. Run the Pipeline
```bash
dbt run
```

5. Run Tests
```bash
dbt test
```

6. Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

## 4. Analytical Models (Marts)

1. Customer Lifetime Value (dim_customers)
Provides a 360-degree view of customers, including lifetime revenue, order counts, and segmentation.

2. Product Performance (mart_product_performance)
- Aggregates sales by product to calculate:
- Return Rate: (Total Returns / Total Sold)
- Gross Profit: (Revenue - Cost)
- Profit Margin: (Profit / Revenue)

3. Monthly Revenue Trends (mart_monthly_revenue)
Time-series aggregation for executive dashboards showing Monthly Active Users (MAU) and Revenue Growth.

4. Orders Fact (fct_orders)
The central transactional table linking customers, products, and return status.

## 5. Sample Analysis & Output

**Query 1: High Value Premium Customers**

```sql
SELECT
    customer_id,
    email,
    customer_segment,
    total_revenue,
    total_orders
FROM dim_customers
WHERE customer_segment = 'Premium'
ORDER BY total_revenue DESC
LIMIT 5;
```

**Query 2: Product Profitability**

```sql
SELECT
    product_name,
    total_revenue,
    return_rate,
    profit_margin
FROM mart_product_performance
ORDER BY profit_margin DESC
LIMIT 5;
```

## 6. Testing & Quality Assurance

### Data quality is enforced via dbt test:
1. Generic Tests: unique, not_null constraints on primary keys (IDs).
2. Relationship Tests: Ensuring user_id in orders exists in the users table.
3. Singular Tests: assert_total_revenue_positive.sql validates that no order has a negative revenue value, protecting against data corruption.

## 7. Reflection & Future Improvements

Time Taken: Approximately 4 hours.

### Challenges:
1. Initial schema mismatch in raw_returns required refactoring the join logic in the intermediate layer.
2. Ensuring the incremental logic for fct_orders correctly handled late-arriving data.

### Future Improvements:
1. Orchestration: Implement a GitHub Actions workflow to run dbt run on every Pull Request (CI/CD).
2. Data Quality: Add dbt-expectations package for more statistical testing (e.g., expecting row counts to be within a range).
3. Visualization: Connect a lightweight BI tool like Metabase or Streamlit to the DuckDB file for live charting.