# Built-in packages
from pathlib import Path
from typing import Dict
import re

# Third-party packages
import pandas as pd
from loguru import logger

DEFAULT_DATA_FOLDER = Path(__file__).parent.parent.parent / "quote_data"

class TosQuoteParser:
    def get_data_files(self, data_folder: Path) -> Dict[str, Path]:
        """Get files matching StrategyReports_ticker_101825 format"""
        assert data_folder.exists(), f"Data folder does not exist: {data_folder}"
        
        pattern = r"StrategyReports_([A-Z]{3,4})_\d+"
        ticker_files = {}
        
        for file_path in data_folder.iterdir():
            if file_path.is_file():
                match = re.match(pattern, file_path.stem)
                if match:
                    ticker = match.group(1)
                    ticker_files[ticker] = file_path
        
        return ticker_files
    
    def convert_tos_strategy_report(self, file_path: Path) -> None:
        """Convert TOS strategy report file to pandas DataFrame"""
        logger.info(f"Processing file: {file_path}")
        data = []
        current_date = None
        
        with open(file_path, 'r') as file:
            for line in file:
                if 'SOHLCP' in line:
                    parts = line.split('|')
                    # Remove commas from prices greater than $999
                    parts = [part.replace(',', '') for part in parts]
                    
                    if len(parts) >= 6 and current_date:
                        open_price = float(parts[2])
                        high_price = float(parts[3])
                        low_price = float(parts[4])
                        close_price = float(parts[5])
                        data.append([current_date, open_price, high_price, low_price, close_price])
                
                elif 'SellClose' in line:
                    columns = line.split(';')
                    if len(columns) >= 6:
                        date_str = columns[5]
                        if '/' in date_str and len(date_str.split('/')[-1]) == 2:
                            parts = date_str.split('/')
                            year = int(parts[-1])
                            full_year = 2000 + year if year < 50 else 1900 + year
                            current_date = f"{parts[0]}/{parts[1]}/{full_year}"
                        else:
                            current_date = date_str
        
        df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close'])
        start_date = df['date'].iloc[0] if len(df) > 0 else 'N/A'
        finish_date = df['date'].iloc[-1] if len(df) > 0 else 'N/A'
        logger.info(f"Created DataFrame with {len(df)} rows from {start_date} to {finish_date}")

if __name__ == "__main__":
    parser = TosQuoteParser()
    ticker_files = parser.get_data_files(DEFAULT_DATA_FOLDER)
    for ticker, file_path in ticker_files.items():
        if file_path.suffix == '.csv':
            parser.convert_tos_strategy_report(file_path)