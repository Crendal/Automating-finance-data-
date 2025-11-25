import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import os
from openpyxl import load_workbook

def get_last_date_from_excel(excel_path, sheet_name="Kospi"):
    """
    Excel 파일에서 마지막 날짜를 읽어오는 함수
    
    Parameters:
    -----------
    excel_path : str
        Excel 파일 경로
    sheet_name : str
        시트 이름
        
    Returns:
    --------
    datetime or None
        마지막 날짜 또는 None
    """
    try:
        # Excel 파일 읽기
        df_existing = pd.read_excel(excel_path, sheet_name=sheet_name)
        
        if df_existing.empty or '날짜' not in df_existing.columns:
            print("기존 데이터가 없거나 '날짜' 컬럼을 찾을 수 없습니다.")
            return None
        
        # 날짜 컬럼을 datetime으로 변환
        df_existing['날짜'] = pd.to_datetime(df_existing['날짜'])
        
        # 마지막 날짜 반환
        last_date = df_existing['날짜'].max()
        print(f"기존 데이터의 마지막 날짜: {last_date.strftime('%Y-%m-%d')}")
        
        return last_date
        
    except FileNotFoundError:
        print(f"파일을 찾을 수 없습니다: {excel_path}")
        return None
    except Exception as e:
        print(f"Excel 파일 읽기 오류: {e}")
        return None

def get_kospi_data(start_date=None, end_date=None):
    """
    코스피 지수 데이터를 FinanceDataReader로 가져오는 함수
    
    Parameters:
    -----------
    start_date : str or datetime
        시작 날짜
    end_date : str or datetime
        종료 날짜 (기본값: 오늘)
        
    Returns:
    --------
    pd.DataFrame
        코스피 데이터
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        print(f"코스피 데이터 수집 중: {start_date} ~ {end_date}")
        
        # FinanceDataReader로 코스피 지수 데이터 가져오기
        df = fdr.DataReader('KS11', start_date, end_date)  # KS11 = 코스피 지수
        
        if df.empty:
            print("수집된 데이터가 없습니다.")
            return pd.DataFrame()
        
        # 인덱스를 컬럼으로 변환
        df = df.reset_index()
        
        # 컬럼명 변경 (기존 Excel 형식에 맞춤)
        column_mapping = {
            'Date': '날짜',
            'Open': 'Open',
            'High': 'High', 
            'Low': 'Low',
            'Close': 'Close',
            'Volume': 'Volume'
        }
        
        df = df.rename(columns=column_mapping)
        
        # Value와 MarketCap 컬럼 추가 (FinanceDataReader에는 없으므로 계산 또는 빈 값)
        df['Value'] = df['Volume'] * df['Close']  # 거래대금 = 거래량 * 종가
        df['MarketCap'] = None  # 시가총액은 별도 계산 필요하므로 일단 빈 값
        
        # 날짜 형식 통일
        df['날짜'] = pd.to_datetime(df['날짜']).dt.strftime('%Y-%m-%d')
        
        print(f"데이터 수집 완료: {len(df)}건")
        return df
        
    except Exception as e:
        print(f"데이터 수집 오류: {e}")
        return pd.DataFrame()

def append_data_to_excel(excel_path, new_data, sheet_name="Kospi"):
    """
    새로운 데이터를 Excel 파일에 추가하는 함수
    
    Parameters:
    -----------
    excel_path : str
        Excel 파일 경로
    new_data : pd.DataFrame
        추가할 새 데이터
    sheet_name : str
        시트 이름
    """
    try:
        # 기존 데이터 읽기
        try:
            df_existing = pd.read_excel(excel_path, sheet_name=sheet_name)
        except:
            df_existing = pd.DataFrame()
        
        if new_data.empty:
            print("추가할 새 데이터가 없습니다.")
            return
        
        # 데이터 합치기
        if df_existing.empty:
            df_combined = new_data
        else:
            df_combined = pd.concat([df_existing, new_data], ignore_index=True)
            # 중복 제거 (날짜 기준)
            df_combined = df_combined.drop_duplicates(subset=['날짜'], keep='last')
            df_combined = df_combined.sort_values('날짜').reset_index(drop=True)
        
        # Excel 파일에 저장
        with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df_combined.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"데이터가 {sheet_name} 시트에 저장되었습니다.")
        print(f"총 {len(df_combined)}건 (새로 추가: {len(new_data)}건)")
        
    except PermissionError:
        print("파일이 사용 중입니다. Excel을 닫고 다시 시도하세요.")
        # 대안: 새 파일명으로 저장
        backup_path = excel_path.replace('.xlsx', '_backup.xlsx')
        with pd.ExcelWriter(backup_path, engine="openpyxl") as writer:
            df_combined.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"대신 {backup_path}로 저장했습니다.")
        
    except Exception as e:
        print(f"Excel 저장 오류: {e}")

def update_kospi_data(excel_path=r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"):
    """
    코스피 데이터를 업데이트하는 메인 함수
    
    Parameters:
    -----------
    excel_path : str
        Excel 파일 경로
    """
    print("="*60)
    print("코스피 지수 데이터 업데이트 시작")
    print("="*60)
    
    # 1. 기존 파일에서 마지막 날짜 확인
    last_date = get_last_date_from_excel(excel_path, "Kospi")
    
    # 2. 시작 날짜 결정
    if last_date is None:
        # 기존 데이터가 없으면 1년 전부터
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        print(f"기존 데이터가 없어서 {start_date}부터 수집합니다.")
    else:
        # 마지막 날짜 다음날부터
        start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        print(f"마지막 날짜 다음날 {start_date}부터 수집합니다.")
    
    # 3. 오늘 날짜
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    # 4. 시작날짜가 오늘보다 이후라면 업데이트할 것이 없음
    if start_date > end_date:
        print("업데이트할 새로운 데이터가 없습니다.")
        return
    
    # 5. 새 데이터 수집
    new_data = get_kospi_data(start_date, end_date)
    
    # 6. Excel에 추가
    if not new_data.empty:
        append_data_to_excel(excel_path, new_data, "Kospi")
        
        # 7. 결과 미리보기
        print("\n새로 추가된 데이터 미리보기:")
        print(new_data.tail())
    else:
        print("새로 추가할 데이터가 없습니다.")
    
    print("\n코스피 데이터 업데이트 완료!")

# 실행
if __name__ == "__main__":
    # Excel 파일 경로 설정
    EXCEL_PATH = r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"
    
    try:
        update_kospi_data(EXCEL_PATH)
    
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
    
    except Exception as e:
        print(f"\n예상치 못한 오류 발생: {e}")
        import traceback
        traceback.print_exc()