import os
from pyspark.sql import SparkSession

# 🔧 Caminho do arquivo Parquet
output_path = "/app/data/output.parquet"

# 🚀 Inicia Spark
spark = SparkSession.builder \
    .appName("SimpleSparkAppend") \
    .getOrCreate()

# 🧾 Novos dados a serem adicionados
new_data = [("Danillo", 4), ("Eveli", 5), ("Emili", 6)]
new_df = spark.createDataFrame(new_data, ["nome", "valor"])

if os.path.exists(output_path):
    print("⚠️ Arquivo já existe. Exibindo conteúdo atual:")
    existing_df = spark.read.parquet(output_path)
    existing_df.show()

    # 🔄 Combina os dados antigos com os novos
    combined_df = existing_df.union(new_df)
else:
    print("📁 Arquivo não existe. Criando novo com os dados.")
    combined_df = new_df

# 💾 Salva os dados combinados sobrescrevendo o arquivo
combined_df.write.mode("overwrite").parquet(output_path)
print("✅ Dados atualizados e salvos com sucesso.")

# 👀 Exibe os dados finais
print("📊 Dados finais no arquivo:")
final_df = spark.read.parquet(output_path)
final_df.show()

# 🧹 Encerra Spark
spark.stop()
