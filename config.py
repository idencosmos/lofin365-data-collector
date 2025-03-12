"""
Configuration module for data fetching applications.
Loads and provides access to settings from .env file.
"""
import os
import ssl
import logging
from pathlib import Path
from dotenv import load_dotenv

class Config:
    """Configuration class to load and access settings from .env file"""
    
    def __init__(self, env_path='.env'):
        """Initialize configuration by loading from .env file"""
        # Load environment variables
        env_file = Path(env_path)
        if env_file.exists():
            load_dotenv(env_path)
        else:
            logging.warning(f"Environment file {env_path} not found. Using default values where possible.")
        
        # API Configuration
        self.api_key = self._get_api_key()
        self.api_base_url = os.getenv('API_BASE_URL', 'https://www.lofin365.go.kr/lf/hub/QWGJK')
        self.api_response_type = os.getenv('API_RESPONSE_TYPE', 'json')
        self.api_max_records_per_request = int(os.getenv('API_MAX_RECORDS_PER_REQUEST', '1000'))
        self.api_max_retries = int(os.getenv('API_MAX_RETRIES', '3'))
        self.api_retry_delay = int(os.getenv('API_RETRY_DELAY', '5'))
        
        # Data Collection Parameters
        self.data_start_year = int(os.getenv('DATA_START_YEAR', '2016'))
        self.data_end_year = int(os.getenv('DATA_END_YEAR', '2024'))
        
        # Database Configuration
        self.db_name = os.getenv('DB_NAME', 'local_finance_data.db')
        self.db_table = os.getenv('DB_TABLE', 'local_finance_data')
        
        # SSL/TLS Configuration
        self.tls_version_str = os.getenv('TLS_VERSION', 'TLSv1_2')
        self.tls_cipher = os.getenv('TLS_CIPHER', 'AES256-SHA256')
        
        # Map string TLS version to ssl constants
        self.tls_version = {
            'TLSv1': ssl.PROTOCOL_TLSv1,
            'TLSv1_1': ssl.PROTOCOL_TLSv1_1 if hasattr(ssl, 'PROTOCOL_TLSv1_1') else ssl.PROTOCOL_TLSv1,
            'TLSv1_2': ssl.PROTOCOL_TLSv1_2 if hasattr(ssl, 'PROTOCOL_TLSv1_2') else ssl.PROTOCOL_TLSv1
        }.get(self.tls_version_str, ssl.PROTOCOL_TLSv1_2)
        
        # Request Headers
        self.headers = {
            "Content-Type": os.getenv('REQUEST_CONTENT_TYPE', 'application/json'),
            "User-Agent": os.getenv('REQUEST_USER_AGENT', 'Mozilla/5.0')
        }
    
    def _get_api_key(self):
        """Retrieve API key from environment variables with validation."""
        api_key = os.getenv('APIKEY')
        if not api_key:
            logging.warning("API key not found in environment variables. "
                           "Set the APIKEY environment variable or update .env file.")
        return api_key
    
    def get_request_params(self, year, exec_date, page=1):
        """Generate request parameters based on year, execution date, and page."""
        if not self.api_key:
            raise ValueError("API key is required but not found in environment variables")
        
        params = {
            "Key": self.api_key,
            "Type": self.api_response_type,
            "pIndex": page,
            "pSize": self.api_max_records_per_request,
            "fyr": str(year),
            "exe_ymd": exec_date.strftime("%Y%m%d")
        }
        return params
    
    def create_ssl_context(self):
        """Create and return an SSL context with the configured settings."""
        context = ssl.SSLContext(self.tls_version)
        context.set_ciphers(self.tls_cipher)
        return context


# Create a default instance for direct imports
config = Config()