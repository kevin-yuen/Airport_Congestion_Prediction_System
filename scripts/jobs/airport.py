from config import ENV, CONFIG
from scripts.utils.spark_session import get_spark


spark = get_spark()

input_path = CONFIG[ENV]['input_path']
print(input_path)
df = spark.read.option('header', True).csv(input_path)

df.show(5, truncate=False)
