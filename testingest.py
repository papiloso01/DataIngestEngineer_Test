import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CocktailDB API Integration") \
    .getOrCreate()

# URL of the API
API_URL = "https://www.thecocktaildb.com/api/json/v1/1/search.php?s="

# Fetch data from API
def fetch_cocktail_data():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()["drinks"]
    else:
        print("Failed to fetch data from CocktailDB API")
        return []
