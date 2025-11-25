# 외국인 증시 순매수 (금액 기준)
import pandas as pd
from datetime import datetime
from pykrx import stock
import os
import inspect

def get_foreign_flow(start: str, end: str, market: str = "KOSPI") -> pd.DataFrame:
    """
    외국인 일별 순매수 금액 시계열을 반환.
    market: "KOSPI" | "KOSDAQ" | "BOTH"
    """
    # 거래대금(금액) 기준으로 변경
    df1 = stock.get_market_trading_value_by_date(start, end, ticker="KOSPI")
    
    if market.upper() == "KOSPI":
        df = df1.copy()
    elif market.upper() == "KOSDAQ":
        df2 = stock.get_market_trading_value_by_date(start, end, ticker="KOSDAQ")
        df = df2.copy()
    elif market.upper() == "BOTH":
        df2 = stock.get_market_trading_value_by_date(start, end, ticker="KOSDAQ")
        # 공통 날짜 인덱스 기준으로 합산
        df = df1.add(df2, fill_value=0)
    else:
        raise ValueError("market 은 'KOSPI' | 'KOSDAQ' | 'BOTH' 중 하나로 지정하세요.")
    
    # 외국인 순매수 금액 (단위: 원)
    s = df["외국인합계"].astype("float")
    out = pd.DataFrame(index=df.index)
    out.index.name = "Date"
    out["외국인 순매수(일별)"] = s
    
    # YTD 누적: 각 해(연도)마다 누적합을 리셋
    y = out.copy()
    y["Year"] = y.index.year
    out["외국인 순매수(YTD 누적)"] = y.groupby("Year")["외국인 순매수(일별)"].cumsum()
    
    # 최근 20 영업일(약 한 달) 누적
    out["외국인 순매수(최근20영업일 누적)"] = (
        out["외국인 순매수(일별)"].rolling(window=20, min_periods=20).sum()
    )
    
    # z-score 계산
    # 전체 기간 기준 (가장 정확)
    historical_mean = out["외국인 순매수(일별)"].mean()
    historical_std = out["외국인 순매수(일별)"].std(ddof=0)
    out["일별 순매수 z(역사적)"] = (out["외국인 순매수(일별)"] - historical_mean) / historical_std
    
    # 최근20영업일 누적의 z-score(60D 기준 분포와 비교 권장)
    roll_mean_60_sum20 = out["외국인 순매수(최근20영업일 누적)"].rolling(60, min_periods=60).mean()
    roll_std_60_sum20  = out["외국인 순매수(최근20영업일 누적)"].rolling(60, min_periods=60).std(ddof=0)
    out["최근20영업일 누적 z(60D)"] = (
        (out["외국인 순매수(최근20영업일 누적)"] - roll_mean_60_sum20) / roll_std_60_sum20
    )
    
    return out.sort_index()

def build_foreign_flow_dashboard(start: str, end: str):
    """
    KOSPI, BOTH(=KOSPI+KOSDAQ) 두 가지 뷰를 같이 산출해서 반환.
    """
    kospi = get_foreign_flow(start, end, market="KOSPI")
    both  = get_foreign_flow(start, end, market="BOTH")
    
    # 컬럼 이름에 접미사로 시장 구분 달기
    kospi = kospi.add_suffix(" [KOSPI]")
    both  = both.add_suffix(" [KOSPI+KOSDAQ]")
    
    # 공통 인덱스 기준으로 합치기
    dash = kospi.join(both, how="outer")
    return dash

def save_csv(df, path):
    # 폴더가 없으면 생성
    os.makedirs(path, exist_ok=True)
    
    # 호출한 곳의 프레임에서 변수명 찾기
    frame = inspect.currentframe().f_back
    for var_name, var_value in frame.f_locals.items():
        if var_value is df:
            filename = f"{var_name}.csv"
            file_path = os.path.join(path, filename)
            df.to_csv(file_path, sep=';', encoding='utf-8-sig', index=False)
            print(f"저장완료: {file_path}")
            return

# 사용 예시
start = "19981207"
end   = datetime.today().strftime("%Y%m%d")

df_foreign = build_foreign_flow_dashboard(start, end)
Kospi_Liquidity = df_foreign

# 인덱스를 문자열로 변환
Kospi_Liquidity.index = Kospi_Liquidity.index.strftime("%Y-%m-%d")

# Excel 파일 저장
path = r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"
with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    Kospi_Liquidity.to_excel(writer, sheet_name="Kospi_Liquidity", index=True)

print("Kospi_Liquidity 데이터 저장 완료")


# Columns (컬럼):

# 외국인 순매수(일별) [KOSPI] → KOSPI 외국인 일일 순매수 금액
# 외국인 순매수(YTD 누적) [KOSPI] → KOSPI 올해 누적
# 외국인 순매수(최근20영업일 누적) [KOSPI] → KOSPI 최근 20일 누적
# 일별 순매수 z(역사적) [KOSPI] → KOSPI 역사적 z-score
# 최근20영업일 누적 z(60D) [KOSPI] → KOSPI 60일 기준 z-score
# 외국인 순매수(일별) [KOSPI+KOSDAQ] → 통합 외국인 일일 순매수 금액
# 외국인 순매수(YTD 누적) [KOSPI+KOSDAQ] → 통합 올해 누적
# 외국인 순매수(최근20영업일 누적) [KOSPI+KOSDAQ] → 통합 최근 20일 누적
# 일별 순매수 z(역사적) [KOSPI+KOSDAQ] → 통합 역사적 z-score
# 최근20영업일 누적 z(60D) [KOSPI+KOSDAQ] → 통합 60일 기준 z-score
