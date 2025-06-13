# ELT-Pipeline-with-Airflow


`  # ❄️ Snowflake + dbt + Airflow ELT Pipeline

This project demonstrates a modern ELT (Extract, Load, Transform) pipeline using **Snowflake** for cloud data warehousing, **dbt** for transformation and modeling, and **Apache Airflow** (via [Astronomer Cosmos](https://www.astronomer.io/docs/astro/runtime-image-architecture)) for orchestration.

## 🔧 Tech Stack

- **Snowflake**: Data warehouse to store and process data.
- **dbt**: SQL-based transformation framework.
- **Airflow (Astronomer Cosmos)**: Workflow orchestration.
- **Docker**: Containerized deployment.
- **GitHub**: Version control and project collaboration.

---

## 📂 Project Structure

```
├── dags/
│   └── dbt_dag.py              # Cosmos-integrated Airflow DAG
├── data_pipeline/
│   ├── models/
│   │   ├── staging/            # Stage-level transformations
│   │   ├── marts/              # Fact and intermediate models
│   │   └── macros/             # Custom macros for reuse
│   ├── tests/                  # Singular tests
│   └── dbt_project.yml         # dbt project config
├── requirements.txt
└── Dockerfile
```

---

## ⚙️ Step-by-Step Setup

### Step 1: Setup Snowflake Environment

```sql
-- create accounts
use role accountadmin;

create warehouse dbt_wh with warehouse_size='x-small';
create database if not exists dbt_db;
create role if not exists dbt_role;

show grants on warehouse dbt_wh;

grant role dbt_role to user jayzern;
grant usage on warehouse dbt_wh to role dbt_role;
grant all on database dbt_db to role dbt_role;

use role dbt_role;

create schema if not exists dbt_db.dbt_schema;

-- clean up
use role accountadmin;

drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
```

---

### Step 2: Configure `dbt_profile.yaml`

```yaml
models:
  snowflake_workshop:
    staging:
      materialized: view
      snowflake_warehouse: dbt_wh
    marts:
      materialized: table
      snowflake_warehouse: dbt_wh
```

---

### Step 3: Create Source and Staging Files

**`models/staging/tpch_sources.yml`**

```yaml
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch', 'orders')
                  field: o_orderkey
```

**`models/staging/stg_tpch_orders.sql`**

```sql
select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from
    {{ source('tpch', 'orders') }}
```

**`models/staging/tpch/stg_tpch_line_items.sql`**

```sql
select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
	l_orderkey as order_key,
	l_partkey as part_key,
	l_linenumber as line_number,
	l_quantity as quantity,
	l_extendedprice as extended_price,
	l_discount as discount_percentage,
	l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}
```

---

### Step 4: Macros – `macros/pricing.sql`

```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```

---

### Step 5: Fact Table and Marts

**`models/marts/int_order_items.sql`**

```sql
select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_items') }} as line_item
        on orders.order_key = line_item.order_key
order by
    orders.order_date
```

**`models/marts/int_order_items_summary.sql`**

```sql
select 
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key
```

**`models/marts/fct_orders.sql`**

```sql
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ref('stg_tpch_orders')}} as orders
join
    {{ref('int_order_items_summary')}} as order_item_summary
        on orders.order_key = order_item_summary.order_key
order by order_date
```

---

### Step 6: dbt Tests

**`models/marts/generic_tests.yml`**

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```

**Singular Tests – `tests/fct_orders_discount.sql`**

```sql
select
    *
from
    {{ref('fct_orders')}}
where
    item_discount_amount > 0
```

**`tests/fct_orders_date_valid.sql`**

```sql
select
    *
from
    {{ref('fct_orders')}}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')
```

---

### Step 7: Airflow Deployment with Cosmos

**Update `Dockerfile`:**

```docker
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```

**Update `requirements.txt`:**

```
astronomer-cosmos
apache-airflow-providers-snowflake
```

**Create Airflow Connection:**

Create `snowflake_conn` in Airflow UI with:

```json
{
  "account": "<account_locator>-<account_name>",
  "warehouse": "dbt_wh",
  "database": "dbt_db",
  "role": "dbt_role",
  "insecure_mode": false
}
```

**Airflow DAG – `dags/dbt_dag.py`:**

```python
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/data_pipeline"),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
```

---

## 🚀 How to Run Locally

```bash
astro dev start
```

Trigger the DAG via Airflow UI.

---

## 📬 Connect With Me
