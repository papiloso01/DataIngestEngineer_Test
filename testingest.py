from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import requests
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CocktailDB Integration with Audit Columns") \
    .getOrCreate()

# Define schema for structured data with audit columns
ingredient_schema = StructType([
    StructField("IngredientName", StringType(), True),
    StructField("Measurement", StringType(), True)
])

cocktail_schema = StructType([
    StructField("CocktailID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Alcoholic", StringType(), True),
    StructField("GlassType", StringType(), True),
    StructField("Ingredients", ArrayType(ingredient_schema), True),
    StructField("Instructions", StringType(), True),
    StructField("DateModified", StringType(), True)  # New audit column
])

# Function to fetch data from the API
def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Map API response to the schema
def map_to_schema(data):
    if not data or "drinks" not in data:
        return []
    
    cocktails = []
    for drink in data["drinks"]:
        ingredients = []
        for i in range(1, 16):  # There are up to 15 ingredients
            ingredient = drink.get(f"strIngredient{i}")
            measurement = drink.get(f"strMeasure{i}")
            if ingredient:
                ingredients.append({"IngredientName": ingredient, "Measurement": measurement})
        
        cocktails.append({
            "CocktailID": int(drink["idDrink"]),
            "Name": drink["strDrink"],
            "Category": drink.get("strCategory"),
            "Alcoholic": drink.get("strAlcoholic"),
            "GlassType": drink.get("strGlass"),
            "Ingredients": ingredients,
            "Instructions": drink.get("strInstructions"),
            "DateModified": drink.get("dateModified")  # New audit column mapping
        })
    return cocktails

# Fetch data from different API endpoints
urls = [
    "https://www.thecocktaildb.com/api/json/v1/1/search.php?s=margarita",
    "https://www.thecocktaildb.com/api/json/v1/1/search.php?f=a",
    "https://www.thecocktaildb.com/api/json/v1/1/lookup.php?i=11007",
    "https://www.thecocktaildb.com/api/json/v1/1/filter.php?i=Gin",
    "https://www.thecocktaildb.com/api/json/v1/1/filter.php?a=Alcoholic",
    "https://www.thecocktaildb.com/api/json/v1/1/filter.php?c=Cocktail"
]

all_cocktails = []
for url in urls:
    api_data = fetch_data(url)
    mapped_data = map_to_schema(api_data)
    all_cocktails.extend(mapped_data)

# Create a DataFrame from the structured data
cocktail_df = spark.createDataFrame(all_cocktails, schema=cocktail_schema)

# Show the DataFrame content
cocktail_df.show(truncate=False)

# Stop Spark Session
spark.stop()