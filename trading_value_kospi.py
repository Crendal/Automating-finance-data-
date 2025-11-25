# Foreign Investor Net Buying Volume (Value-based)
import pandas as pd
from datetime import datetime
from pykrx import stock
import os
import inspect

def get_foreign_flow(start: str, end: str, market: str = "KOSPI") -> pd.DataFrame:
    """
    Returns a time series of daily foreign investor net buying volume in KRW.
    
    Args:
        start (str): Start date in YYYYMMDD format
        end (str): End date in YYYYMMDD format
        market (str): Market type - "KOSPI" | "KOSDAQ" | "BOTH"
    
    Returns:
        pd.DataFrame: Foreign net buying data with multiple metrics
    """
    # Fetch trading value data (business days only)
    df1 = stock.get_market_trading_value_by_date(start, end, ticker="KOSPI")
    
    if market.upper() == "KOSPI":
        df = df1.copy()
    elif market.upper() == "KOSDAQ":
        df2 = stock.get_market_trading_value_by_date(start, end, ticker="KOSDAQ")
        df = df2.copy()
    elif market.upper() == "BOTH":
        df2 = stock.get_market_trading_value_by_date(start, end, ticker="KOSDAQ")
        # Combine based on common date index
        df = df1.add(df2, fill_value=0)
    else:
        raise ValueError("market must be one of 'KOSPI' | 'KOSDAQ' | 'BOTH'")
    
    # Extract foreign investor net buying amount (unit: KRW)
    s = df["외국인합계"].astype("float")
    out = pd.DataFrame(index=df.index)
    out.index.name = "Date"
    out["Foreign Net Buying (Daily)"] = s
    
    # YTD Cumulative: Reset cumulative sum for each year
    y = out.copy()
    y["Year"] = y.index.year
    out["Foreign Net Buying (YTD Cumulative)"] = y.groupby("Year")["Foreign Net Buying (Daily)"].cumsum()
    
    # Recent 20 trading days (approximately 1 month) cumulative
    out["Foreign Net Buying (Recent 20 Trading Days Cumulative)"] = (
        out["Foreign Net Buying (Daily)"].rolling(window=20, min_periods=20).sum()
    )
    
    # Z-score calculation
    # Based on entire historical period (most accurate)
    historical_mean = out["Foreign Net Buying (Daily)"].mean()
    historical_std = out["Foreign Net Buying (Daily)"].std(ddof=0)
    out["Daily Net Buying Z-score (Historical)"] = (out["Foreign Net Buying (Daily)"] - historical_mean) / historical_std
    
    # Z-score of recent 20 trading days cumulative (recommended to compare with 60D distribution)
    roll_mean_60_sum20 = out["Foreign Net Buying (Recent 20 Trading Days Cumulative)"].rolling(60, min_periods=60).mean()
    roll_std_60_sum20  = out["Foreign Net Buying (Recent 20 Trading Days Cumulative)"].rolling(60, min_periods=60).std(ddof=0)
    out["Recent 20 Trading Days Cumulative Z-score (60D)"] = (
        (out["Foreign Net Buying (Recent 20 Trading Days Cumulative)"] - roll_mean_60_sum20) / roll_std_60_sum20
    )
    
    return out.sort_index()

def build_foreign_flow_dashboard(start: str, end: str):
    """
    Build a comprehensive dashboard with both KOSPI and KOSPI+KOSDAQ views.
    
    Args:
        start (str): Start date in YYYYMMDD format
        end (str): End date in YYYYMMDD format
    
    Returns:
        pd.DataFrame: Combined dashboard with dual market perspectives
    """
    kospi = get_foreign_flow(start, end, market="KOSPI")
    both  = get_foreign_flow(start, end, market="BOTH")
    
    # Add market suffix to column names for distinction
    kospi = kospi.add_suffix(" [KOSPI]")
    both  = both.add_suffix(" [KOSPI+KOSDAQ]")
    
    # Merge based on common index
    dash = kospi.join(both, how="outer")
    return dash

def save_csv(df, path):
    """
    Save dataframe to CSV file with variable name as filename.
    
    Args:
        df (pd.DataFrame): Dataframe to save
        path (str): Directory path for saving
    """
    # Create folder if it doesn't exist
    os.makedirs(path, exist_ok=True)
    
    # Find variable name from calling frame
    frame = inspect.currentframe().f_back
    for var_name, var_value in frame.f_locals.items():
        if var_value is df:
            filename = f"{var_name}.csv"
            file_path = os.path.join(path, filename)
            df.to_csv(file_path, sep=';', encoding='utf-8-sig', index=False)
            print(f"Save completed: {file_path}")
            return

# Usage example
start = "19981207"
end   = datetime.today().strftime("%Y%m%d")

df_foreign = build_foreign_flow_dashboard(start, end)
kospi_liquidity = df_foreign

# Convert index to string format (only once!)
kospi_liquidity.index = kospi_liquidity.index.strftime("%Y-%m-%d")

# Save to Excel file
# Please change the path
path = r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"
with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    kospi_liquidity.to_excel(writer, sheet_name="Kospi_Liquidity", index=True)

print("Kospi_Liquidity data saved successfully!")
