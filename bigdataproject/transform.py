import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure

def transform_data(connection_string: str) -> pd.DataFrame:
    """
    Retrieves data from MongoDB, performs basic cleaning, and prepares it for further transformation.

    Args:
        connection_string (str): MongoDB connection string.

    Returns:
        pd.DataFrame: Cleaned and transformed DataFrame.
    """
    try:
        # Connect to MongoDB Atlas
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        db = client["InsiderTransactionsDB"]
        collection = db["transactions"]

        # Retrieve data from MongoDB
        cursor = collection.find()
        data = list(cursor)

        if not data:
            print("No data retrieved from MongoDB. Returning an empty DataFrame.")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        print(f"Retrieved {len(df)} rows from MongoDB.")

        # Basic data cleaning
        if "_id" in df.columns:
            df.drop(["_id"], axis=1, inplace=True)  # Drop MongoDB's ObjectID column

        # Encoding categorical variables
        if "acquisition_or_disposal" in df.columns:
            df["acquisition_or_disposal"] = df["acquisition_or_disposal"].map(
                {"A": 1, "D": 0}
            )

        # Drop unnecessary columns
        columns_to_drop = ["executive", "Company Symbol"]
        existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        df.drop(existing_columns_to_drop, axis=1, inplace=True)

        print("Basic data transformation completed.")
        return df

    except ConnectionFailure:
        print("Failed to connect to MongoDB Atlas. Please check your connection string.")
        return pd.DataFrame()
    except PyMongoError as pme:
        print(f"A PyMongo error occurred: {pme}")
        return pd.DataFrame()
    except Exception as e:
        print("An unexpected error occurred during data transformation:")
        print(e)
        return pd.DataFrame()