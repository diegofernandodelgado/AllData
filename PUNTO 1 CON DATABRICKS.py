# Databricks notebook source
## DESAFIO 1 - 2 CON DATABRICKS
file_location_dep = "/FileStore/tables/departamentos.csv"
file_location_emp = "/FileStore/tables/empleados.csv"
file_location_pto = "/FileStore/tables/puestos.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

df_dept = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location_dep)

df_emp = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location_emp)

df_pto = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location_pto)

#display(df_dept)
#display(df_emp)
#display(df_pto)

# COMMAND ----------

## DESAFIO 3

temp_table_dep = "departamentos_csv"
temp_table_emp = "empleados_csv"
temp_table_pto = "puestos_csv"

df_dept.createOrReplaceTempView(temp_table_dep)
df_emp.createOrReplaceTempView(temp_table_emp)
df_pto.createOrReplaceTempView(temp_table_pto)

# COMMAND ----------

## DESAFIO 3

spark.sql("""
    CREATE TABLE IF NOT EXISTS dep
    USING DELTA
    AS SELECT * FROM `departamentos_csv`
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS puestos
    USING DELTA
    AS SELECT * FROM `puestos_csv`
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS emp
    USING DELTA
    AS SELECT * FROM `empleados_csv`
""")

# COMMAND ----------

##DESAFIO 4

result = spark.sql("""
    SELECT  A.*,B.* 
      FROM dep AS A, emp AS B, puestos AS C 
     WHERE  A.codigo_departamento_indec=C.codigo_departamento_indec AND B.puestos=C.puestos
""")
result.show()
