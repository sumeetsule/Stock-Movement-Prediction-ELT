import requests
import pandas as pd
from typing import List

def extract_data(api_key: str, company_symbols: List[str]) -> pd.DataFrame:
    """
    Fetches insider transaction data for the given company symbols using the provided API key.

    Args:
        api_key (str): API key for Alpha Vantage.
        company_symbols (List[str]): List of company symbols to fetch data for.

    Returns:
        pd.DataFrame: Combined DataFrame containing data for all companies. Returns an empty DataFrame if an error occurs.
    """
    combined_df = pd.DataFrame()

    for symbol in company_symbols:
        try:
            url = f"https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol={symbol}&apikey={api_key}"
            response = requests.get(url, timeout=10)

            # Check for HTTP errors
            response.raise_for_status()

            data = response.json()

            # Check if the expected key exists
            if "data" in data:
                transactions_data = data["data"]
                if transactions_data:  # Ensure there's data to process
                    df = pd.json_normalize(transactions_data)
                    df["Company Symbol"] = symbol
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                else:
                    print(f"No transactions data available for {symbol}.")
            else:
                print(f"No 'data' key found in the response for {symbol}. Response: {data}")

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred while fetching data for {symbol}: {http_err}")
        except requests.exceptions.ConnectionError:
            print(f"Connection error occurred while fetching data for {symbol}.")
        except requests.exceptions.Timeout:
            print(f"Request timed out for {symbol}.")
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred while fetching data for {symbol}: {req_err}")
        except ValueError as val_err:
            print(f"Error decoding JSON response for {symbol}: {val_err}")
        except Exception as e:
            print(f"An unexpected error occurred for {symbol}: {e}")

    if combined_df.empty:
        print("No data was extracted. Returning an empty DataFrame.")

    return combined_df
