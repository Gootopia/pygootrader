# Built-in packages
from pathlib import Path
from typing import Dict, List
import re

# Third-party packages
import pandas as pd
from loguru import logger
from influx_database import InfluxDatabase, InstrumentTags

DEFAULT_DATA_FOLDER = Path(__file__).parent.parent.parent / "quote_data"

class TosQuoteParser:
    TOS_TIMESTAMP_KEY = "date"
    
    @classmethod
    def get_data_files(
        cls,
        data_folder: Path = DEFAULT_DATA_FOLDER,
        use_tos_report: bool = True
    ) -> Dict[str, Path]:
        """Get files matching StrategyReports_ticker_mmddyy format"""
        assert data_folder.exists(), f"Data folder does not exist: {data_folder}"
        
        pattern = r"StrategyReports_([A-Za-z]{3,4})_\d+\.csv"

        if use_tos_report == False:
            pattern = r"([A-Za-z]{3,4})\.csv"
            
        ticker_files = {}
        
        for file_path in data_folder.iterdir():
            if file_path.is_file():
                match = re.match(pattern, file_path.name)
                if match:
                    ticker = match.group(1)
                    ticker_files[ticker] = file_path
        
        return ticker_files
    
    @classmethod
    def correct_year(cls, df: pd.DataFrame) -> pd.DataFrame:
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
    def write_to_database(
        cls,
        db: InfluxDatabase = None,
        data_folder: Path = DEFAULT_DATA_FOLDER,
        files: List[Path] = None
    ) -> None:
        """Write processed data to database"""
        assert db is not None, "Database connection is required"
        
        if files is None:
            logger.info("No files provided, will process all files in the folder")
            ticker_files = cls.get_data_files(data_folder, use_tos_report=False)
            files = list(ticker_files.values())
        
        assert len(files) > 0, "No files found to process"
        
        for file_path in files:
            df_quote = pd.read_csv(file_path)
            ticker = file_path.stem.lower()
            tags = InstrumentTags(symbol=ticker)
            db.write_pandas(dataframe=df_quote,tags=tags,timestamp_key=cls.TOS_TIMESTAMP_KEY)


    @classmethod
    def process_folder(cls, folder: Path = DEFAULT_DATA_FOLDER) -> list:
        """Process all TOS strategy report files in a folder"""
        logger.info(f"Processing folder: {folder}")
        ticker_files = cls.get_data_files(folder)
        processed_files = []
        
        if not ticker_files:
            logger.warning("No ticker files found in the folder")
            return processed_files
        
        for ticker, file_path in ticker_files.items():
            if file_path.suffix == '.csv':
                logger.info(f"{'='*50}")
                logger.info(f"🔄 PROCESSING TICKER: {ticker.upper()}")
                logger.info(f"{'='*50}")
                converted_file = cls.convert_tos_strategy_report(file_path)
                processed_files.append(converted_file)
        
        logger.info(f"Processed {len(processed_files)} files: {[f.name for f in processed_files]}")
        return processed_files
    
    @classmethod
    def convert_tos_strategy_report(cls, file_path: Path, generate_csv: bool = True) -> Path:
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
        adjusted_df = cls.correct_year(df)
        finish_date = adjusted_df['date'].iloc[0] if len(adjusted_df) > 0 else 'N/A'
        start_date = adjusted_df['date'].iloc[-1] if len(adjusted_df) > 0 else 'N/A'
        logger.info(f"Created DataFrame with {len(adjusted_df)} rows from {start_date} to {finish_date}")
        
        output_filename = f"{symbol}.csv"
        output_path = file_path.parent / output_filename
        
        if generate_csv:
            adjusted_df.to_csv(output_path, index=False)
            logger.info(f"CSV file generated: {output_path}")
        
        return output_path

if __name__ == "__main__":
    db = InfluxDatabase()
    processed_files = TosQuoteParser.process_folder()
    TosQuoteParser.write_to_database(db)