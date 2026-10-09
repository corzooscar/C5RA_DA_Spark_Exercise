from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc

# Inicializar Spark
spark = SparkSession.builder \
    .appName("AnalisisVentasEcommerce") \
    .getOrCreate()

print("¡Spark iniciado con éxito! Versión:", spark.version)

# 2. Leer el conjunto de datos
df = spark.read.csv("./csv/sales.csv", header=True, inferSchema=True)

# 3. Filtrar registros válidos e inválidos según las reglas de negocio
valid_condition = (
    col("order_id").isNotNull() &
    col("order_date").isNotNull() &
    col("customer_id").isNotNull() &
    col("city").isNotNull() &
    col("product").isNotNull() &
    col("quantity").isNotNull() & (col("quantity") > 0) &
    col("unit_price").isNotNull() & (col("unit_price") > 0)
)

df_validos = df.filter(valid_condition)
df_invalidos = df.exceptAll(df_validos)

# 4. Calcular el ingreso por transacción para ventas válidas
df_validos = df_validos.withColumn("revenue", col("quantity") * col("unit_price"))

# 5. Generar resúmenes agrupados
resumen_producto = df_validos.groupBy("product").agg(
    spark_sum("quantity").alias("unidades_vendidas"),
    spark_sum("revenue").alias("ingreso_total")
)

resumen_ciudad = df_validos.groupBy("city").agg(
    spark_sum("revenue").alias("ingreso_total")
)

# 6. Identificar métricas principales (Acciones sobre el DataFrame)
total_procesados = df.count()
total_validos = df_validos.count()
total_invalidos = df_invalidos.count()

ingreso_total = df_validos.select(spark_sum("revenue")).collect()[0][0]
unidades_totales = df_validos.select(spark_sum("quantity")).collect()[0][0]

producto_top = resumen_producto.orderBy(desc("ingreso_total")).first()
ciudad_top = resumen_ciudad.orderBy(desc("ingreso_total")).first()

# 7. Mostrar resumen en consola
print("-" * 40)
print("INDICADORES GENERALES DEL PROCESAMIENTO")
print("-" * 40)
print(f"Total de registros procesados: {total_procesados}")
print(f"Registros aceptados (válidos): {total_validos}")
print(f"Registros rechazados (inválidos): {total_invalidos}")
print(f"Ingreso total de ventas válidas: ${ingreso_total:,.0f}")
print(f"Total de unidades vendidas: {unidades_totales}")
print(f"Producto con mayor ingreso: {producto_top['product']} (${producto_top['ingreso_total']:,.0f})")
print(f"Ciudad con mayor ingreso: {ciudad_top['city']} (${ciudad_top['ingreso_total']:,.0f})")
print("-" * 40)

# 8. Guardar los resultados generados en archivos de salida
df_validos.write.csv("output/ventas_validas", header=True, mode="overwrite")
df_invalidos.write.csv("output/ventas_invalidas", header=True, mode="overwrite")
resumen_producto.write.csv("output/resumen_producto", header=True, mode="overwrite")
resumen_ciudad.write.csv("output/resumen_ciudad", header=True, mode="overwrite")