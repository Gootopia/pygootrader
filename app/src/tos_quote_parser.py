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
        """Get files matching StrategyReports_ticker_mmddyy format"""
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
    
    def correct_year(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert year from 2 digit to 4 digit value"""
        if len(df) == 0:
            return df
        
        for i in range(1, len(df)):
            current_year = int(df.iloc[i]['date'].split('/')[-1])
            previous_year = int(df.iloc[i-1]['date'].split('/')[-1])
            
            if current_year > previous_year:
                for j in range(i, len(df)):
                    date_parts = df.iloc[j]['date'].split('/')
                    year = int(date_parts[-1]) - 100
                    df.iloc[j, df.columns.get_loc('date')] = f"{date_parts[0]}/{date_parts[1]}/{year}"
                break
        
        return df
    
    @classmethod
    def process_folder(cls, folder: Path = DEFAULT_DATA_FOLDER) -> None:
        """Process all TOS strategy report files in a folder"""
        logger.info(f"Processing folder: {folder}")
        parser = TosQuoteParser()
        ticker_files = parser.get_data_files(folder)
        
        if not ticker_files:
            logger.warning("No ticker files found in the folder")
            return
        
        for ticker, file_path in ticker_files.items():
            if file_path.suffix == '.csv':
                logger.info(f"{'='*50}")
                logger.info(f"🔄 PROCESSING TICKER: {ticker.upper()}")
                logger.info(f"{'='*50}")
                parser.convert_tos_strategy_report(file_path)
    
    def convert_tos_strategy_report(self, file_path: Path, generate_csv: bool = True) -> None:
        """Convert TOS strategy report file to pandas DataFrame"""
        logger.info(f"Processing file: {file_path}")
        symbol = file_path.stem.split('_')[1].lower()
        logger.info(f"Extracted symbol: {symbol}")
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
                            full_year = 2000 + year
                            current_date = f"{parts[0]}/{parts[1]}/{full_year}"
                        else:
                            current_date = date_str
        
        df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close'])
        df = df.iloc[::-1].reset_index(drop=True)
        adjusted_df = self.correct_year(df)
        finish_date = adjusted_df['date'].iloc[0] if len(adjusted_df) > 0 else 'N/A'
        start_date = adjusted_df['date'].iloc[-1] if len(adjusted_df) > 0 else 'N/A'
        logger.info(f"Created DataFrame with {len(adjusted_df)} rows from {start_date} to {finish_date}")
        
        if generate_csv:
            output_path = file_path.parent / f"{symbol}.csv"
            adjusted_df.to_csv(output_path, index=False)
            logger.info(f"CSV file generated: {output_path}")

if __name__ == "__main__":
    TosQuoteParser.process_folder()