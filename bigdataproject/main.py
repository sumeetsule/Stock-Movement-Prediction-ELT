import os
from dotenv import load_dotenv
from pandas import DataFrame
from typing import List
from extract import extract_data
from load import load_data
from transform import transform_data

# Load environment variables
load_dotenv()

if __name__ == "__main__":
    """
    Main function to run the ELT (Extract, Load, Transform) process.
    Extracts insider transactions data, loads it into MongoDB, and performs basic transformations.
    """
    print("Starting ELT process...")

    print("\n")

    # Step 1: Extract
    print("Step 1: Extracting data...")
    api_key: str = os.getenv("ALPHAVANTAGE_API_KEY", "")
    if not api_key:
        raise ValueError("API key is not set in the .env file.")

    # Company symbols to iterate and fecth data for that symbol
    company_symbols: List[str] = ["META", "AMZN", "AAPL", "NFLX", "GOOGL", "MSFT", "IBM", "NVDA", 
           "AXP", "UBER", "DASH", "TSLA", "JPM", "BLK", "WFC", "DAL"]

    # Extract data from Alpha Vantage API for each company symbol
    combined_df: DataFrame = extract_data(api_key, company_symbols)
    print(f"Data extraction completed. Extracted {len(combined_df)} rows.")

    print("\n")

    # Step 2: Load
    print("Step 2: Loading data into MongoDB Atlas...")
    connection_string: str = os.getenv("MONGO_CONNECTION_STRING", "")
    if not connection_string:
        raise ValueError("MongoDB connection string is not set in the .env file.")

    # Load the extracted data into MongoDB and get the client instance
    client = load_data(combined_df, connection_string)
    if client:
        print("Data loading completed, and MongoDB connection is active.")
    else:
        print("Data loading failed. Check logs for details.")
        exit(1)  # Exit if loading fails

    print("\n")

    # Step 3: Transform
    print("Step 3: Transforming data...")
    transformed_df: DataFrame = transform_data(connection_string)
    print(
        f"Data transformation completed. Transformed DataFrame has {len(transformed_df)} rows."
    )

    # Close the MongoDB connection after all operations
    print("Closing MongoDB connection...")
    client.close()
    print("MongoDB connection closed.")
    
    print("\n")
    
    print("ELT process completed successfully.")