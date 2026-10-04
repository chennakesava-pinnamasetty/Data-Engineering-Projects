dspark.conf.set("fs.azure.account.auth.type.stproductsretail.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.stproductsretail.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.stproductsretail.dfs.core.windows.net",clientID)
spark.conf.set("fs.azure.account.oauth2.client.secret.stproductsretail.dfs.core.windows.net", secertid)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.stproductsretail.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantID}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls("abfss://bronze@stproductsretail.dfs.core.windows.net"))

# COMMAND ----------

# DBTITLE 1,read the bronze layer
# Read raw data from Bronze layer
df_customers = spark.read.format("parquet").option("recursiveFileLookup", "true").load("abfss://bronze@stproductsretail.dfs.core.windows.net/customer/")
df_stores = spark.read.format("parquet").option("recursiveFileLookup", "true").load("abfss://bronze@stproductsretail.dfs.core.windows.net/store/")
df_products = spark.read.format("parquet").option("recursiveFileLookup", "true").load("abfss://bronze@stproductsretail.dfs.core.windows.net/products/")
df_transactions = spark.read.format("parquet").option("recursiveFileLookup", "true").load("abfss://bronze@stproductsretail.dfs.core.windows.net/transaction/")


# COMMAND ----------

display(df_transactions)

# COMMAND ----------

# DBTITLE 1,create silver layer - data cleaning
from pyspark.sql.functions import col

# Convert types and clean data
df_transactions = df_transactions.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

df_products = df_products.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double")
)

df_stores = df_stores.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

df_customers = df_customers.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])


# COMMAND ----------
