# Built-in packages
from pathlib import Path
from typing import List

# Third-party packages
import pandas as pd
from loguru import logger

DEFAULT_DATA_FOLDER = Path(__file__).parent.parent.parent / "quote_data"

class QuoteParser:
    def get_data_files(self, data_folder: Path) -> List[Path]:
        """Get all files in the specified data folder"""
        assert data_folder.exists(), f"Data folder does not exist: {data_folder}"
        
        file_list = list(data_folder.iterdir())
        return file_list
    
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
        logger.info(f"Created DataFrame with {len(df)} rows")

if __name__ == "__main__":
    parser = QuoteParser()
    files = parser.get_data_files(DEFAULT_DATA_FOLDER)
    for file in files:
        if file.suffix == '.csv':
            parser.convert_tos_strategy_report(file)