# COMMAND ----------
# ARCAS - Optimizacion de tablas Delta
# Ejecutar UNA SOLA VEZ manualmente desde el notebook o desde un Job separado
# No incluir en el pipeline diario

# 1. Particionado de TBL_ALERTS por status y fecha
# Las queries de la app filtran siempre por status (pending/approved/etc)
# y por fecha (created_at). El particionado elimina full scans.

spark.sql("""
    CREATE TABLE IF NOT EXISTS arcas_processed.alerts_optimized
    USING DELTA
    PARTITIONED BY (status, date(created_at))
    AS SELECT * FROM arcas_processed.alerts
""")

# Si la tabla ya tiene datos, recrearla particionada:
# spark.sql("DROP TABLE IF EXISTS arcas_processed.alerts_optimized")
# spark.sql("""
#     CREATE TABLE arcas_processed.alerts_optimized
#     USING DELTA
#     PARTITIONED BY (status, date(created_at))
#     AS SELECT * FROM arcas_processed.alerts
# """)

# 2. Particionado de TBL_ARTICLES por source_type y pub_date
# Las queries de la app filtran siempre por source_type != gazette
# y por ingested_at (para el cuadro de mandos por dias)

spark.sql("""
    CREATE TABLE IF NOT EXISTS arcas_raw.articles_optimized
    USING DELTA
    PARTITIONED BY (source_type, pub_date)
    AS SELECT * FROM arcas_raw.articles
""")

# 3. OPTIMIZE + ZORDER en ambas tablas
# ZORDER coloca fisicamente juntos los datos mas consultados juntos
# Reduce el numero de archivos que Spark lee en cada query

spark.sql("""
    OPTIMIZE arcas_processed.alerts
    ZORDER BY (status, created_at, confidence_score)
""")

spark.sql("""
    OPTIMIZE arcas_raw.articles
    ZORDER BY (source_type, ingested_at, topic)
""")

spark.sql("OPTIMIZE arcas_processed.entities ZORDER BY (entity_type, last_seen)")

# 4. VACUUM — elimina versiones antiguas de Delta (>7 dias)
# Libera espacio en el Free Tier de Databricks

spark.sql("VACUUM arcas_processed.alerts RETAIN 168 HOURS")
spark.sql("VACUUM arcas_raw.articles RETAIN 168 HOURS")
spark.sql("VACUUM arcas_processed.entities RETAIN 168 HOURS")

print("Optimizacion completada")

# 5. Estadisticas de tablas para que el optimizador de Spark
# pueda hacer mejores planes de ejecucion

spark.sql("ANALYZE TABLE arcas_processed.alerts COMPUTE STATISTICS FOR ALL COLUMNS")
spark.sql("ANALYZE TABLE arcas_raw.articles COMPUTE STATISTICS FOR ALL COLUMNS")

print("Estadisticas actualizadas")
print("\nTamanio actual de tablas:")
for tbl in ["arcas_raw.articles","arcas_processed.alerts",
            "arcas_processed.entities","arcas_processed.relations"]:
    try:
        n = spark.sql(f"SELECT count(*) AS n FROM {tbl}").collect()[0]["n"]
        print(f"  {tbl}: {n:,} rows")
    except Exception as e:
        print(f"  {tbl}: {e}")
