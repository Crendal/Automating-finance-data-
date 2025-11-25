# -*- coding: utf-8 -*-
import os
import re
import time
import random
from typing import List
from urllib.parse import unquote
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import xlwings as xw
import warnings

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

warnings.filterwarnings('ignore')

SMBS_URL = "http://www.smbs.biz/Exchange/FxSwapUS.jsp"

# ==================== 웹 크롤링 관련 함수들 ====================
_script_pat = re.compile(r"""d[1-9]\s*\(\s*'(.*?)'\s*\)\s*;?""", re.S)

def _decode_obfuscated(s: str) -> str:
    s = re.sub(r'%(?:u)?_([A-Z])', r'%', s)
    s = s.replace('%u_', '%u').replace('%_', '%')
    
    def _unquote_u(match: re.Match) -> str:
        hexcode = match.group(1)
        try:
            return chr(int(hexcode, 16))
        except Exception:
            return match.group(0)
    
    s = re.sub(r'%u([0-9a-fA-F]{4})', _unquote_u, s)
    s = unquote(s)
    return s.strip()

def _cell_text(tag) -> str:
    scr = tag.find('script')
    if scr and scr.string:
        m = _script_pat.search(scr.string)
        if m:
            return _decode_obfuscated(m.group(1))
        return _decode_obfuscated(scr.get_text(" ", strip=True))
    return tag.get_text(" ", strip=True)

def _parse_table(html: str, date_str: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    target_tbl = None
    for tbl in soup.find_all("table"):
        cap = tbl.find("caption")
        cap_txt = cap.get_text(strip=True) if cap else ""
        if "F/X Swap POINT 결과 표" in cap_txt:
            target_tbl = tbl
            break
    
    if target_tbl is None:
        return pd.DataFrame()

    thead = target_tbl.find("thead")
    if thead:
        headers = [_cell_text(th) for th in thead.find_all("th")]
    else:
        first_tr = target_tbl.find("tr")
        headers = [_cell_text(x) for x in first_tr.find_all(["th", "td"])]

    rows = []
    tbody = target_tbl.find("tbody")
    if not tbody:
        return pd.DataFrame()
    
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        row = [_cell_text(td) for td in tds]
        if row:
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=headers)
    num_cols = [c for c in df.columns if c != df.columns[0]]
    for c in num_cols:
        df[c] = (
            df[c].astype(str)
                 .str.replace(",", "", regex=False)
                 .str.replace("\u2212", "-", regex=False)
                 .str.replace("−", "-", regex=False)
        )
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df.insert(0, "date", date_str)
    return df

def _build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=ko-KR")
    drv = webdriver.Chrome(options=opts)
    drv.set_page_load_timeout(30)
    return drv

def _input_date_step_by_step(driver: webdriver.Chrome, date_str: str):
    # 페이지 하단 스크롤
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(0.5)
    
    try:
        search_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="searchDate"]'))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", search_input)
        time.sleep(0.3)
        search_input.click()
        time.sleep(0.3)
        
        # 기존 내용 완전 삭제
        driver.execute_script("arguments[0].value = '';", search_input)
        search_input.send_keys(Keys.CONTROL + "a")
        search_input.send_keys(Keys.DELETE)
        search_input.clear()
        for _ in range(20):
            search_input.send_keys(Keys.BACK_SPACE)
        time.sleep(0.2)
        
        # 날짜 입력
        search_input.send_keys(date_str)
        time.sleep(0.2)
        
        # 조회 버튼 클릭
        search_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="frm_SearchDate"]/p[4]/a/img'))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", search_btn)
        time.sleep(0.3)
        search_btn.click()
        time.sleep(0.5)
        
        return True
        
    except TimeoutException:
        return False

def get_business_days_list(start_date: str, end_date: str) -> List[str]:
    """pandas BDay를 사용해서 영업일 리스트 생성"""
    start = pd.to_datetime(start_date, format="%Y.%m.%d")
    end = pd.to_datetime(end_date, format="%Y.%m.%d")
    
    # 영업일 범위 생성
    business_days = pd.bdate_range(start=start, end=end)
    
    return [d.strftime("%Y.%m.%d") for d in business_days]

def fetch_fx_swap_points_range_selenium(start_date: str, end_date: str | None = None, headless: bool = True) -> pd.DataFrame:
    """주어진 기간의 FX Swap Point 데이터를 크롤링"""
    if end_date is None:
        end_date = datetime.today().strftime("%Y.%m.%d")
    
    # pandas를 사용해서 영업일 리스트 생성
    business_days_list = get_business_days_list(start_date, end_date)
    
    if not business_days_list:
        return pd.DataFrame()

    driver = _build_driver(headless=headless)
    dfs: List[pd.DataFrame] = []
    
    try:
        driver.get(SMBS_URL)
        time.sleep(2)
        
        for date_str in business_days_list:
            date_str_input = date_str.replace(".", "")  # YYYYMMDD 형식으로 변환
            
            success = _input_date_step_by_step(driver, date_str_input)
            if not success:
                continue

            time.sleep(1.5)
            df_day = _parse_table(driver.page_source, date_str)
            
            if not df_day.empty:
                dfs.append(df_day)
            
            time.sleep(random.uniform(0.3, 0.7))

    finally:
        driver.quit()

    if not dfs:
        return pd.DataFrame()

    out = pd.concat(dfs, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"], format="%Y.%m.%d", errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").set_index("date")

    if len(out.columns) > 0:
        key_cols = [out.columns[0]]
        out = (
            out.reset_index()
               .drop_duplicates(subset=["date"] + key_cols, keep="last")
               .set_index("date")
               .sort_index()
        )
    
    return out

def calculate_mid_values(df_all):
    """Bid/Offer 데이터에서 Mid 값 계산"""
    mid_rows = []
    for date in df_all.index.unique():
        date_data = df_all.loc[df_all.index == date].copy()
        bid_row = date_data[date_data['Side'].str.contains('bid', case=False, na=False)]
        offer_row = date_data[date_data['Side'].str.contains('offer', case=False, na=False)]
        
        if len(bid_row) > 0 and len(offer_row) > 0:
            mid_row = bid_row.iloc[0:1].copy()
            mid_row.loc[mid_row.index[0], 'Side'] = 'mid'
            
            numeric_cols = ["1M", "2M", "3M", "6M", "1Y"]
            for col in numeric_cols:
                bid_val = pd.to_numeric(bid_row[col].iloc[0], errors='coerce')
                offer_val = pd.to_numeric(offer_row[col].iloc[0], errors='coerce')
                
                if pd.notna(bid_val) and pd.notna(offer_val):
                    mid_val = (bid_val + offer_val) / 2
                    mid_row.loc[mid_row.index[0], col] = mid_val
                else:
                    mid_row.loc[mid_row.index[0], col] = np.nan
            
            mid_rows.append(mid_row)

    if mid_rows:
        return pd.concat(mid_rows, ignore_index=False).sort_index()
    else:
        return pd.DataFrame()

# ==================== 데이터 업데이트 관련 함수들 ====================
def get_next_business_day(date):
    """주어진 날짜의 다음 영업일 반환"""
    if isinstance(date, str):
        date = pd.to_datetime(date).date()
    elif hasattr(date, 'date'):
        date = date.date()
    
    next_day = date + timedelta(days=1)
    while next_day.weekday() >= 5:  # 0=월요일, 6=일요일
        next_day += timedelta(days=1)
    return next_day

def save_to_excel(df, excel_path, sheet_name):
    """DataFrame을 Excel 시트에 저장"""
    try:
        wb = xw.Book(excel_path)
        ws = wb.sheets[sheet_name]
        ws.clear()
        df_with_index = df.reset_index()
        ws.range("A1").value = df_with_index
        wb.save()
        print(f"Excel 저장 완료: {sheet_name} 시트")
        return True
    except Exception as e:
        print(f"Excel 저장 실패: {e}")
        return False

def update_fx_swap_incremental(csv_file="fx_swap_mid.csv", save_csv=True, excel_path=None, sheet_name=None):
    """
    기존 CSV의 마지막 날짜부터 오늘까지 FX Swap 데이터 업데이트
    
    Parameters:
    - csv_file: 기존 CSV 파일 경로
    - save_csv: CSV 파일 저장 여부
    - excel_path: Excel 파일 경로 (저장하지 않으려면 None)
    - sheet_name: Excel 시트명 (저장하지 않으려면 None)
    
    Returns:
    - pd.DataFrame: 업데이트된 전체 데이터
    """
    try:
        # 1. 기존 데이터 확인
        if not os.path.exists(csv_file):
            print(f"기존 CSV 파일({csv_file})이 없습니다. 전체 수집이 필요합니다.")
            return None
        
        # 2. 기존 데이터 로드
        existing_df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        last_date = existing_df.index[-1]
        
        if isinstance(last_date, str):
            last_date = pd.to_datetime(last_date)
        
        print(f"기존 데이터 마지막 날짜: {last_date.strftime('%Y-%m-%d')}")
        
        # 3. 업데이트 날짜 범위 계산
        start_date = last_date + pd.tseries.offsets.BDay(1)  # 다음 영업일
        end_date = pd.Timestamp.today()
        
        if start_date > end_date:
            print("업데이트할 새로운 영업일이 없습니다.")
            return existing_df
        
        print(f"업데이트 범위: {start_date.strftime('%Y.%m.%d')} ~ {end_date.strftime('%Y.%m.%d')}")
        
        # 4. 새 데이터 수집
        df_new = fetch_fx_swap_points_range_selenium(
            start_date=start_date.strftime("%Y.%m.%d"),
            end_date=end_date.strftime("%Y.%m.%d"),
            headless=True
        )
        
        if df_new.empty:
            print("새로운 데이터 수집 실패 또는 새 데이터가 없음")
            return existing_df
        
        df_new.columns = ["Side", "1M", "2M", "3M", "6M", "1Y"]
        df_new_mid = calculate_mid_values(df_new)
        
        if df_new_mid.empty:
            print("Mid 값 계산 실패")
            return existing_df
        
        # 5. 데이터 병합
        combined_df = pd.concat([existing_df, df_new_mid], axis=0)
        combined_df = combined_df.sort_index()
        
        # 중복 제거 (같은 날짜는 최신 것만 유지)
        combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
        
        print(f"새로 추가된 데이터: {len(df_new_mid)}행")
        print(f"전체 데이터: {len(combined_df)}행")
        
        # 6. CSV 저장 (옵션)
        if save_csv:
            combined_df.to_csv(csv_file)
            print(f"CSV 저장 완료: {csv_file}")
        else:
            print("CSV 저장 건너뜀")
        
        # 7. Excel 저장 (옵션)
        if excel_path and sheet_name:
            save_to_excel(combined_df, excel_path, sheet_name)
        
        return combined_df
        
    except Exception as e:
        print(f"업데이트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_existing_data(csv_file="fx_swap_mid.csv"):
    """기존 데이터 로드"""
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        print(f"데이터 로드: {len(df)} rows")
        print(f"데이터 범위: {df.index.min().strftime('%Y-%m-%d')} ~ {df.index.max().strftime('%Y-%m-%d')}")
        return df
    else:
        print(f"CSV 파일이 없습니다: {csv_file}")
        return pd.DataFrame()

def check_data_status(csv_file="fx_swap_mid.csv"):
    """데이터 상태 확인"""
    print("=== 데이터 상태 확인 ===")
    
    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
        last_date = df.index.max().date()
        next_business = get_next_business_day(last_date)
        today = datetime.now().date()
        
        print(f"마지막 데이터 날짜: {last_date.strftime('%Y-%m-%d')}")
        print(f"다음 업데이트 시작일: {next_business.strftime('%Y-%m-%d')}")
        print(f"오늘 날짜: {today.strftime('%Y-%m-%d')}")
        
        # 업데이트 가능한 영업일 계산
        business_days = pd.bdate_range(start=next_business, end=today)
        print(f"업데이트 가능한 영업일: {len(business_days)}일")
        
        if len(business_days) > 0:
            print("업데이트가 필요합니다.")
        else:
            print("데이터가 최신 상태입니다.")
    else:
        print("기존 데이터 파일이 없습니다. 전체 수집이 필요합니다.")

# ==================== 메인 실행 부분 ====================
if __name__ == "__main__":
    # 데이터 상태 확인
    check_data_status()
    
    print("\n" + "="*50)
    
    # 데이터 조회만 (저장 안함)
    updated_df = update_fx_swap_incremental(
        csv_file="fx_swap_mid.csv",
        save_csv=False,  # CSV 저장 안함
        excel_path=None,  # Excel 저장 안함
        sheet_name=None   # 시트 저장 안함
    )
    
    if updated_df is not None:
        print("\n=== 최근 5행 데이터 ===")
        print(updated_df.tail())
        
        print(f"\n=== 데이터 정보 ===")
        print(f"총 행수: {len(updated_df)}")
        print(f"기간: {updated_df.index.min().strftime('%Y-%m-%d')} ~ {updated_df.index.max().strftime('%Y-%m-%d')}")
        print(f"컬럼: {list(updated_df.columns)}")
        
        # 업데이트된 데이터를 기존 CSV 파일에 저장
        print("\n" + "="*50)
        print("업데이트된 데이터를 CSV 파일에 저장 중...")
        try:
            updated_df.to_csv("fx_swap_mid.csv", encoding='utf-8-sig')
            print("✓ 기존 CSV 파일 업데이트 완료: fx_swap_mid.csv")
        except Exception as e:
            print(f"✗ CSV 저장 실패: {e}")
            
    else:
        print("데이터 업데이트 실패")


Swap_Point=updated_df



import os
import inspect

path = r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"
import pandas as pd
# Swap_Point.index = pd.to_datetime(Swap_Point.index).strftime("%Y-%m-%d")
print(Swap_Point)
with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    Swap_Point.to_excel(writer, sheet_name="Swap_Point", index=True)

