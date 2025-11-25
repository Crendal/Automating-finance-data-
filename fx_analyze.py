import pandas as pd
from openbb import obb
# start_dates="2020-01-01"
# today_str= datetime.now().strftime("%Y-%m-%d")
from datetime import datetime
from dateutil.relativedelta import relativedelta
# today's date as string
today_str = datetime.now().strftime("%Y-%m-%d")
# date one month before today
start_dates = "2009-12-28"
# (datetime.now() - relativedelta(months=2520)).strftime("%Y-%m-%d")
end_dates=today_str
obb.user.preferences.output_type = "dataframe"
provider = "yfinance"
# --- 1) Define symbols ---
core = ["KRW=X"]
asia = ["CNY=X", "TWD=X", "THB=X", "SGD=X", "MYR=X", "IDR=X", "INR=X", "PHP=X", "HKD=X"]
g10  = ["JPY=X", "CHF=X", "CAD=X", "NOK=X", "SEK=X", "EURUSD=X", "GBPUSD=X", "AUDUSD=X", "NZDUSD=X"]
# Dollar index symbols (INDEX route; DO NOT add '=X')
dxy_symbols = ["DX-Y.NYB"]  # you can keep both; whichever returns will be used
fx_pairs = sorted(set(core + asia + g10))         # FX-only (end with '=X')
index_syms = dxy_symbols                          # Index-only
# --- 2) Fetch FX via currency API ---
data_fx = None
missing_fx = []
for sym in fx_pairs:
    try:
        s = obb.currency.price.historical(symbol=sym, provider=provider, start_date=start_dates,
    end_date=end_dates,interval="1d")["close"].rename(sym)
        data_fx = pd.concat([data_fx, s], axis=1) if data_fx is not None else s.to_frame()
    except Exception as e:
        missing_fx.append(sym)
        print(f"{sym} : missing (fx) -> {e}")
# --- 3) Fetch Dollar Index via INDEX API (try both, no caret/ticker munging) ---
got_index = False
for sym in index_syms:
    try:
        s = obb.index.price.historical(
            symbol=sym, provider=provider, use_cache=False, start_date=start_dates,
    end_date=end_dates,interval="1d"
        )["close"].rename(sym)
        data_fx = s.to_frame() if data_fx is None else data_fx.join(s, how="outer")
        got_index = True
        print(f"Loaded index: {sym}")
    except Exception as e:
        print(f"{sym} : missing (index) -> {e}")
# --- 4) Final shape: rows=tickers, cols=dates ---
if data_fx is None:
    data_fx = pd.DataFrame()
data_fx = data_fx.sort_index()
fx_matrix = data_fx.transpose().sort_index(axis=1)
# fx_matrix.to_csv("fx_matrix.csv")
# print("Saved: fx_matrix.csv")
# if missing_fx: print("Missing FX tickers:", missing_fx)

# 심볼 이름 매핑 딕셔너리 # symbol & name mapping
symbol_rename_map = {
    'AUDUSD=X': 'AUD_USD',
    'CAD=X': 'USD_CAD', 
    'CHF=X': 'USD_CHF',
    'CNY=X': 'USD_CNY',
    'EURUSD=X': 'EUR_USD',
    'GBPUSD=X': 'GBP_USD', 
    'IDR=X': 'USD_IDR',
    'INR=X': 'USD_INR',
    'JPY=X': 'USD_JPY',
    'KRW=X': 'USD_KRW',
    'MYR=X': 'USD_MYR',
    'NOK=X': 'USD_NOK',
    'NZDUSD=X': 'NZD_USD',
    'PHP=X': 'USD_PHP',
    'SEK=X': 'USD_SEK',
    'SGD=X': 'USD_SGD',
    'THB=X': 'USD_THB',
    'TWD=X': 'USD_TWD',
    'HKD=X': 'USD_HKD',
    'DX-Y.NYB': 'DXY'
}
# 인덱스 이름 변경 # changing the name of the index
fx_matrix.index = fx_matrix.index.map(symbol_rename_map)
# fx_matrix
fx_matrix_clean = fx_matrix.ffill(axis=1)  # 이전 영업일 데이터로 채움

# EUR/USD → USD/EUR 변환 
fx_matrix_clean.loc['USD_EUR'] = 1 / fx_matrix_clean.loc['EUR_USD']
# GBP/USD → USD/GBP 변환  
fx_matrix_clean.loc['USD_GBP'] = 1 / fx_matrix_clean.loc['GBP_USD']
fx_matrix_clean.loc['USD_AUD'] = 1 / fx_matrix_clean.loc['AUD_USD']
# GBP/USD → USD/GBP 변환  
fx_matrix_clean.loc['USD_NZD'] = 1 / fx_matrix_clean.loc['NZD_USD']
fx_matrix_clean = fx_matrix_clean.drop(index=['NZD_USD','EUR_USD','AUD_USD','GBP_USD'])
fx_matrix_clean




import numpy as np
def calculate_basic_metrics(fx_data):
    """
    기본 FX 메트릭 계산
    """
    latest_date = fx_data.columns[-1]
    
    # 주간: 5영업일 전
    week_ago_idx = max(0, len(fx_data.columns) - 6)
    week_ago = fx_data.columns[week_ago_idx]
    
    # 월간: 22영업일 전  
    month_ago_idx = max(0, len(fx_data.columns) - 22)
    month_ago = fx_data.columns[month_ago_idx]
    
    # YTD: 2025년 첫 영업일
    year_start_cols = [col for col in fx_data.columns if col.year == 2025]
    ytd_start = year_start_cols[0] if year_start_cols else fx_data.columns[0]
    
    results = []
    
    for currency in fx_data.index:
        current_price = fx_data.loc[currency, latest_date]
        week_price = fx_data.loc[currency, week_ago]
        month_price = fx_data.loc[currency, month_ago]
        ytd_price = fx_data.loc[currency, ytd_start]
        
        # 수익률 계산
        wow_change = ((current_price / week_price) - 1) * 100
        mom_change = ((current_price / month_price) - 1) * 100
        ytd_change = ((current_price / ytd_price) - 1) * 100
        
        # 전고점 및 MDD
        currency_data = fx_data.loc[currency]
        all_time_high = currency_data.max()
        ath_distance = ((current_price / all_time_high) - 1) * 100
        
        running_max = currency_data.expanding().max()
        drawdown = ((currency_data / running_max) - 1) * 100
        max_drawdown = drawdown.min()
        
        # RSI
        returns = currency_data.pct_change().dropna()
        delta = returns.diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        current_rsi = rsi.iloc[-1] if len(rsi) > 0 else np.nan
        
        # 변동성 (21일)
        if len(returns) >= 21:
            vol_21d = returns.rolling(21).std().iloc[-1] * np.sqrt(252) * 100
        else:
            vol_21d = np.nan
        
        results.append({
            'Currency': currency,
            'Current': round(current_price, 4),
            'WoW(%)': f"{round(wow_change, 2)}%",
            'MoM(%)': f"{round(mom_change, 2)}%", 
            'YTD(%)': f"{round(ytd_change, 2)}%",
            'Deviation from 15-year High (%)': f"{round(ath_distance, 2)}%",
            'MDD(%)': f"{round(max_drawdown, 2)}%",
            'RSI': round(current_rsi, 1) if not pd.isna(current_rsi) else np.nan,
            'Vol(%)': f"{round(vol_21d, 2)}%" if not pd.isna(vol_21d) else np.nan
        })
                    
    return pd.DataFrame(results)

def create_regional_dashboards(fx_matrix_clean):
    """
    지역별 대시보드 생성
    """
    print("Calculating FX metrics...")
    
    # 전체 메트릭 계산
    full_dashboard = calculate_basic_metrics(fx_matrix_clean)
    
    # 지역별 통화 정의 (실제 데이터에 맞게)
    g10_currencies = ['DXY','USD_EUR', 'USD_GBP', 'USD_JPY', 'USD_CHF', 'USD_CAD', 
                     'USD_AUD', 'USD_NZD', 'USD_NOK', 'USD_SEK']
    
    asia_currencies = ['USD_CNY', 'USD_TWD', 'USD_THB', 'USD_SGD', 'USD_MYR', 
                      'USD_IDR', 'USD_INR', 'USD_PHP', 'USD_HKD', 'USD_KRW']
    
    # 실제 데이터에 있는 통화들 확인
    available_currencies = list(full_dashboard['Currency'].unique())
    print(f"📊 Available currencies: {available_currencies}")
    
    # G10 데이터 필터링
    g10_data = full_dashboard[full_dashboard['Currency'].isin(g10_currencies)].copy()
    g10_data = g10_data.sort_values('YTD(%)', ascending=False)
    
    # Asia 데이터 필터링  
    asia_data = full_dashboard[full_dashboard['Currency'].isin(asia_currencies)].copy()
    asia_data = asia_data.sort_values('YTD(%)', ascending=False)
    
    # KRW 별도 추출
    krw_data = full_dashboard[full_dashboard['Currency'] == 'USD_KRW'].copy()
    
    # DXY 별도 추출
    dxy_data = full_dashboard[full_dashboard['Currency'] == 'DXY'].copy()
    
    return {
        'g10': g10_data,
        'asia': asia_data, 
        'krw': krw_data,
        'dxy': dxy_data,
        'full': full_dashboard
    }
# 대시보드 생성
dashboards = create_regional_dashboards(fx_matrix_clean)

dashboards['asia']=dashboards['asia'].reset_index(drop=True)
dashboards['g10']=dashboards['g10'].reset_index(drop=True)
dashboards['full']=dashboards['full'].reset_index(drop=True)

# dashboards['g10']
import xlwings as xw

# 저장 대상 경로
file_path = r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"

# 저장할 DF 준비 (필요 시 인덱스 제거/정리)
df_asia = dashboards['asia'].copy()
df_g10  = dashboards['g10'].copy()
df_full = dashboards['full'].copy()

# G10 인덱스 재정렬
g10_order = ['DXY', 'USD_EUR', 'USD_JPY', 'USD_GBP', 'USD_CAD', 
             'USD_SEK', 'USD_CHF', 'USD_NOK', 'USD_AUD', 'USD_NZD']
    
# ASIA 인덱스 재정렬  
asia_order = ['USD_CNY', 'USD_INR', 'USD_KRW', 'USD_IDR', 'USD_TWD', 
              'USD_THB', 'USD_SGD', 'USD_MYR', 'USD_PHP', 'USD_HKD']
# G10 데이터프레임 정렬
df_g10['sort_key'] = df_g10['Currency'].map({curr: idx for idx, curr in enumerate(g10_order)})
df_g10 = df_g10.sort_values('sort_key').drop('sort_key', axis=1).reset_index(drop=True)

# ASIA 데이터프레임 정렬
df_asia['sort_key'] = df_asia['Currency'].map({curr: idx for idx, curr in enumerate(asia_order)})
df_asia = df_asia.sort_values('sort_key').drop('sort_key', axis=1).reset_index(drop=True)       


import os
import inspect

path = r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"
import pandas as pd

with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_g10.to_excel(writer, sheet_name="g10", index=False)
with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    df_asia.to_excel(writer, sheet_name="asia", index=False)
with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    fx_matrix_clean.to_excel(writer, sheet_name="FX_Data", index=True)


