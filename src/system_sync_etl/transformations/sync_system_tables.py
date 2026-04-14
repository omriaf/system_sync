from pyspark import pipelines as dp

SKIP_SCHEMAS = {"information_schema"}
SOURCE_CATALOG = "system"
TARGET_CATALOG = "system_sync"


def check_table_readable(fqn):
    """Verify we have permission to read a table by running a zero-row query."""
    try:
        spark.sql(f"SELECT * FROM {fqn} LIMIT 0").collect()
        return True
    except Exception:
        print(f"SKIP (no access): {fqn}")
        return False


def discover_and_prepare():
    """Dynamically discover schemas/tables and create target schemas."""
    tables = []
    schemas_df = spark.sql(f"SHOW SCHEMAS IN {SOURCE_CATALOG}")
    for schema_row in schemas_df.collect():
        schema_name = schema_row.databaseName
        if schema_name in SKIP_SCHEMAS:
            continue

        try:
            tables_df = spark.sql(f"SHOW TABLES IN {SOURCE_CATALOG}.{schema_name}")
            schema_has_readable = False
            for table_row in tables_df.collect():
                table_name = table_row.tableName
                fqn = f"{SOURCE_CATALOG}.{schema_name}.{table_name}"
                if check_table_readable(fqn):
                    tables.append((schema_name, table_name))
                    schema_has_readable = True

            if schema_has_readable:
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.`{schema_name}`")
        except Exception as e:
            print(f"WARN: Could not process schema {SOURCE_CATALOG}.{schema_name}: {e}")

    return tables


def create_sync_table(schema_name, table_name):
    """Factory function — avoids Python closure-over-loop-variable bug."""
    source_fqn = f"{SOURCE_CATALOG}.{schema_name}.{table_name}"

    @dp.table(
        name=f"{TARGET_CATALOG}.{schema_name}.{table_name}",
        comment=f"Daily full sync of {source_fqn}",
    )
    def sync():
        return spark.read.table(source_fqn)

    return sync


# Dynamic discovery, schema creation, and table generation at pipeline planning time
discovered_tables = discover_and_prepare()
print(f"Discovered {len(discovered_tables)} readable system tables to sync")

for schema_name, table_name in discovered_tables:
    try:
        create_sync_table(schema_name, table_name)
    except Exception as e:
        print(f"WARN: Could not create sync for {SOURCE_CATALOG}.{schema_name}.{table_name}: {e}")


# Derived table: account_prices for periods they cover, list_prices for all other periods

@dp.table(
    name=f"{TARGET_CATALOG}.billing.effective_prices",
    comment="Effective prices: uses account_prices where available by time range, falls back to list_prices for periods not covered",
)
def effective_prices():
    return spark.sql(f"""
        WITH account_rows AS (
            SELECT
                account_id,
                price_start_time,
                price_end_time,
                sku_name,
                cloud,
                currency_code,
                usage_unit,
                try_variant_get(to_variant_object(pricing), '$.default', 'decimal(38,18)') AS price,
                'account_prices' AS source_table
            FROM {TARGET_CATALOG}.billing.account_prices
        ),
        list_rows AS (
            SELECT
                account_id,
                price_start_time,
                price_end_time,
                sku_name,
                cloud,
                currency_code,
                usage_unit,
                try_variant_get(to_variant_object(pricing), '$.effective_list.default', 'decimal(38,18)') AS price,
                'list_prices' AS source_table
            FROM {TARGET_CATALOG}.billing.list_prices l
            WHERE NOT EXISTS (
                SELECT 1 FROM account_rows a
                WHERE a.sku_name = l.sku_name
                AND a.cloud = l.cloud
                AND a.usage_unit = l.usage_unit
                AND a.account_id = l.account_id
                AND l.price_start_time < COALESCE(a.price_end_time, TIMESTAMP '9999-01-01')
                AND a.price_start_time < COALESCE(l.price_end_time, TIMESTAMP '9999-01-01')
            )
        )
        SELECT * FROM account_rows
        UNION ALL
        SELECT * FROM list_rows
    """)
