from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure, PyMongoError
import pandas as pd

def load_data(combined_df: pd.DataFrame, connection_string: str) -> MongoClient:
    """
    Loads data from a DataFrame into a MongoDB Atlas collection.
    
    Args:
        combined_df (pd.DataFrame): DataFrame containing the data to be inserted into MongoDB.
        connection_string (str): MongoDB Atlas connection string for the database.
        
    Returns:
        MongoClient: The MongoDB client instance to keep the connection open for further use.
    """
    try:
        # Validate input DataFrame
        if combined_df.empty:
            print("The DataFrame is empty. No data to insert.")
            return None

        # Convert DataFrame to a list of dictionaries
        data_dict = combined_df.to_dict("records")

        # Establish MongoDB connection
        print("Connecting to MongoDB Atlas...")
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        db = client["InsiderTransactionsDB"]
        collection = db["transactions"]

        # Test MongoDB connection
        client.admin.command('ping')
        print("MongoDB connection established successfully.")

        # Drop all existing documents in the collection
        collection.delete_many({})
        print("Existing documents in the collection have been cleared.")

        # Insert new data
        collection.insert_many(data_dict)
        print(f"Data successfully inserted into MongoDB! Total records: {len(data_dict)}.")

        # Confirm data insertion
        record_count = collection.count_documents({})
        example_doc = collection.find_one()
        num_columns = len(example_doc.keys()) if example_doc else 0
        print(f"MongoDB Collection Info: {record_count} rows, {num_columns} columns.")

        # Return the MongoDB client to keep the connection open
        return client

    except BulkWriteError as bwe:
        print("Bulk Write Error occurred during data insertion:")
        print(bwe.details)
    except ConnectionFailure:
        print("Failed to connect to MongoDB Atlas. Please check your connection string.")
    except PyMongoError as pme:
        print(f"A PyMongo error occurred: {pme}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None