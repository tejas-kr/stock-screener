import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from stock_screener.stock_screener import app

client = TestClient(app)

class TestStockScreenerAPI:

    def test_health_check_success(self):
        """Test health check endpoint when database is accessible"""
        with patch('stock_screener.stock_screener.get_db_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            mock_get_conn.return_value = mock_conn

            response = client.get("/health")
            assert response.status_code == 200
            assert response.json() == {"status": "healthy"}

    def test_health_check_db_failure(self):
        """Test health check endpoint when database connection fails"""
        with patch('stock_screener.stock_screener.get_db_connection') as mock_get_conn:
            mock_get_conn.side_effect = Exception("Connection failed")

            response = client.get("/health")
            assert response.status_code == 500
            assert "Database connection failed" in response.json()["detail"]

    @patch('stock_screener.stock_screener.NiftyIndexSaver')
    def test_grab_csvs_success(self, mock_nifty_saver_class):
        """Test grab CSV files endpoint success"""
        mock_saver_instance = MagicMock()
        mock_nifty_saver_class.return_value = mock_saver_instance

        response = client.post("/grab-csvs")
        assert response.status_code == 200
        assert response.json() == {"message": "CSV files downloaded successfully"}
        mock_saver_instance.scrape_and_download.assert_called_once()

    @patch('stock_screener.stock_screener.NiftyIndexSaver')
    def test_grab_csvs_failure(self, mock_nifty_saver_class):
        """Test grab CSV files endpoint failure"""
        mock_nifty_saver_class.side_effect = Exception("Download failed")

        response = client.post("/grab-csvs")
        assert response.status_code == 500
        assert "Failed to download CSV files" in response.json()["detail"]

    @patch('stock_screener.stock_screener.glob.glob')
    @patch('stock_screener.stock_screener.get_db_connection')
    def test_populate_stocks_success(self, mock_get_conn, mock_glob):
        """Test populate stocks endpoint success"""
        # Mock CSV files
        mock_glob.return_value = ['test.csv']

        # Mock CSV reading
        mock_csv_data = [
            {'Symbol': 'TEST1', 'Company Name': 'Test Company 1', 'Industry': 'Tech', 'ISIN Code': 'IN123'},
            {'Symbol': 'TEST2', 'Company Name': 'Test Company 2', 'Industry': 'Finance', 'ISIN Code': 'IN456'}
        ]

        with patch('builtins.open', create=True) as mock_open:
            mock_file = MagicMock()
            mock_reader = MagicMock()
            mock_reader.__iter__.return_value = mock_csv_data
            mock_file.__enter__.return_value = mock_file
            mock_file.__iter__.return_value = mock_csv_data
            mock_open.return_value.__enter__.return_value = mock_file

            with patch('csv.DictReader', return_value=mock_reader):
                mock_conn = MagicMock()
                mock_cursor = MagicMock()
                mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
                mock_get_conn.return_value = mock_conn

                response = client.post("/populate-stocks")
                assert response.status_code == 200
                assert "Populated stocks table with 2 records" in response.json()["message"]

    @patch('stock_screener.stock_screener.glob.glob')
    def test_populate_stocks_no_csvs(self, mock_glob):
        """Test populate stocks endpoint when no CSV files found"""
        mock_glob.return_value = []

        response = client.post("/populate-stocks")
        assert response.status_code == 404
        assert "No CSV files found" in response.json()["detail"]

    @patch('stock_screener.stock_screener.get_db_connection')
    @patch('stock_screener.stock_screener.StockRepository')
    @patch('stock_screener.stock_screener.ValueReferenceService')
    def test_populate_valuation_references_success(self, mock_service_class, mock_repo_class, mock_get_conn):
        """Test populate valuation references endpoint success"""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_repo_instance = MagicMock()
        mock_repo_class.return_value = mock_repo_instance
        mock_repo_instance.get_all_stocks_as_list.return_value = ['TEST1', 'TEST2']

        mock_service_instance = MagicMock()
        mock_service_class.return_value = mock_service_instance

        response = client.post("/populate-valuation-references")
        assert response.status_code == 200
        assert "Populated valuation references for 2 stocks" in response.json()["message"]
        mock_service_instance.run.assert_called_once_with(['TEST1', 'TEST2'])

    @patch('stock_screener.stock_screener.get_db_connection')
    @patch('stock_screener.stock_screener.StockRepository')
    def test_populate_valuation_references_no_stocks(self, mock_repo_class, mock_get_conn):
        """Test populate valuation references when no stocks found"""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_repo_instance = MagicMock()
        mock_repo_class.return_value = mock_repo_instance
        mock_repo_instance.get_all_stocks_as_list.return_value = []

        response = client.post("/populate-valuation-references")
        assert response.status_code == 404
        assert "No stocks found" in response.json()["detail"]

    @patch('stock_screener.stock_screener.get_db_connection')
    @patch('stock_screener.stock_screener.DiscountScreenerService')
    def test_populate_valuation_snapshots_success(self, mock_service_class, mock_get_conn):
        """Test populate valuation snapshots endpoint success"""
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        mock_service_instance = MagicMock()
        mock_service_class.return_value = mock_service_instance

        response = client.post("/populate-valuation-snapshots")
        assert response.status_code == 200
        assert response.json() == {"message": "Populated valuation snapshots"}
        mock_service_instance.run.assert_called_once()