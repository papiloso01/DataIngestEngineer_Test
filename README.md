# DataIngestEngineer_Test

The code retrieves data from TheCocktailDB API into a tabular format using Spark DataFrames. First step was to import spark components and functions for data transformation. Thereafter, create requests to fetch API data and process the response returned by the API.

Schema Definitions
In the code, I defined two schemas to handle data retrieved from the API:
1)	Ingredient Schema: to capture the ingredient name and corresponding measurement.

2)	Cocktail Schema to capture cocktail fields:
-	CocktailID: Unique ID for the drink.
-	Name: Name of the cocktail.
-	Category: Category of the cocktail (e.g., "Ordinary Drink").
-	Alcoholic: Specifies if the drink contains alcohol.
-	GlassType: Type of glass recommended.
-	Ingredients: An array of `ingredient_schema` entries.
-	Instructions: Steps to prepare the drink.
-	DateModified: date of last changes in the row

Functions 
1)	Fetch Data: Takes an API URL as input and uses the request library to fetch the data with the conditional statement ‘If the API call is successful (HTTP 200 OK), the response is returned in JSON format otherwise, `None` is returned.’
2)	Map API Response to Schema (performs data mapping)
a)	This function checks if the API response contains a valid `"drinks"` key otherwise, it returns an empty list. 
b)	Iterates through each drink in the `data["drinks"]` and extracts up to 15 ingredients and their corresponding measurements.
c)	Creates a dictionary for each field and returns a list of formatted cocktail data.

API Endpoints and Data Processing
I defined a list of selected endpoints (urls) to be used in fetching the data based on the search criteria. Also, the functions are triggered for each URL and the transformed data is added to ‘all_cocktails’.

Key Points
1. Data Fetching: The script fetches drink data from multiple endpoints of TheCocktailDB API.
2. Data Mapping: The data is converted into a structured format using well-defined schemas.
3. Scalable Processing: Spark can process the structured data efficiently if integrated into a DataFrame in subsequent steps (not shown here).
