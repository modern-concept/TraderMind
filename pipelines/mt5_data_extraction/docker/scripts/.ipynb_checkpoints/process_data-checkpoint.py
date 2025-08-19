import os
from pyspark.sql import SparkSession

# 🔧 Caminho do arquivo
output_path = "/app/data/output.parquet"

# 🚀 Inicia Spark
spark = SparkSession.builder \
    .appName("SimpleSparkTest") \
    .getOrCreate()

if not os.path.exists(output_path):
    # 🧾 Dados fictícios
    data = [("Danillo", 1), ("Eveli", 2)]
    df = spark.createDataFrame(data, ["nome", "valor"])
    
    # 💾 Salva o DataFrame
    df.write.mode("overwrite").parquet(output_path)
    print("✅ Arquivo salvo com sucesso.")
else:
    print("⚠️ Arquivo já existe. Exibindo conteúdo:")
    
    # 📂 Lê o arquivo existente e exibe
    df = spark.read.parquet(output_path)
    df.show()

# 🧹 Encerra Spark
spark.stop()