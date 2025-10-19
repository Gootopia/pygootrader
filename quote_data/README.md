# Quote Data

This folder contains scripts to extract data from TOS downloads.

## File Types

- **Strategy Reports**: CSV files containing TOS (Think or Swim) strategy report data with OHLC (Open, High, Low, Close) price information
- **Format**: Files contain paired data rows where SellClose rows provide dates and SOHLCP rows provide price data
- **TOS Strategy**: The `tos_data_collect.tos` file is a ThinkScript strategy that can be added to a TOS chart to generate the data collection reports

### Instructions
1. Import the tos_data_collect strategy into ThinkOrSwim
2. Add strategy to a chart
3. Right click on the menu when the strategy is active (indicated by lots of little orders in white)
4. Save the data to a local folder
5. Use tos_quote_parser.py to process

## Usage

Files in this folder are processed by the `tos_quote_parser.py` module to extract and convert trading data into pandas DataFrames for analysis.

## Reference

For more information on exporting historical data from ThinkOrSwim, see: [Exporting Historical Data from ThinkOrSwim for External Analysis](https://usethinkscript.com/threads/exporting-historical-data-from-thinkorswim-for-external-analysis.507/)

This link provides methods and scripts for extracting OHLC data directly from the TOS platform.