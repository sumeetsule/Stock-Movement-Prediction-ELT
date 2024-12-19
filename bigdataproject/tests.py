import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from extract import extract_data
from load import load_data
from transform import transform_data


class TestETLProcess(unittest.TestCase):

    @patch("extract.requests.get")
    def test_extract_data(self, mock_get):
        # Mock API response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "data": [
                {"executive": "John Doe", "acquisition_or_disposal": "A"},
                {"executive": "Jane Doe", "acquisition_or_disposal": "D"},
            ]
        }

        api_key = "test_api_key"
        company_symbols = ["META"]
        result = extract_data(api_key, company_symbols)

        # Check the DataFrame is not empty
        self.assertFalse(result.empty)
        self.assertEqual(len(result), 2)

    @patch("load.MongoClient")
    def test_load_data(self, mock_mongo_client):
        # Mock MongoDB client and collection
        mock_client = MagicMock()
        mock_collection = mock_client["InsiderTransactionsDB"]["transactions"]
        mock_mongo_client.return_value = mock_client

        # Create a sample DataFrame
        data = {"executive": ["John Doe"], "acquisition_or_disposal": ["A"]}
        df = pd.DataFrame(data)

        connection_string = "mongodb+srv://test:test@cluster0.mongodb.net/test"
        result_client = load_data(df, connection_string)

        # Check if insert_many was called with the correct data
        mock_collection.insert_many.assert_called_once_with(df.to_dict("records"))

        # Ensure the function executes without errors
        self.assertIsNotNone(result_client)

    @patch("transform.MongoClient")
    def test_transform_data(self, mock_mongo_client):
        # Mock MongoDB client and collection
        mock_client = MagicMock()
        mock_collection = mock_client["InsiderTransactionsDB"]["transactions"]
        mock_mongo_client.return_value = mock_client

        # Mock MongoDB find() response
        mock_collection.find.return_value = [
            {"_id": "1", "acquisition_or_disposal": "A", "executive": "John Doe"},
            {"_id": "2", "acquisition_or_disposal": "D", "executive": "Jane Doe"},
        ]

        connection_string = "mongodb+srv://test:test@cluster0.mongodb.net/test"
        transformed_df = transform_data(connection_string)

        # Check the DataFrame is not empty and has the expected columns
        self.assertFalse(transformed_df.empty)
        self.assertIn("acquisition_or_disposal", transformed_df.columns)
        self.assertNotIn("_id", transformed_df.columns)
        self.assertNotIn("executive", transformed_df.columns)


if __name__ == "__main__":
    unittest.main()