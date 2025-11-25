import time
import pandas as pd
import os
import glob
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class KMBRateCrawler:
    def __init__(self, download_path="C:\\Users\\jesst\\Downloads", headless=False):
        """
        KMB 금리 데이터 크롤러 초기화
        
        Parameters:
        -----------
        download_path : str
            파일 다운로드 경로
        headless : bool
            브라우저를 숨김 모드로 실행할지 여부
        """
        self.base_url = 'https://www.kmbco.com/kor/rate/deri_rate.do'
        self.download_path = download_path
        self.driver = None
        self.downloaded_files = []  # 다운로드된 파일 추적
        self.setup_driver(headless)
        
    def setup_driver(self, headless):
        """Chrome 드라이버 설정"""
        options = Options()
        if headless:
            options.add_argument('--headless')
        
        # 창 크기 최대화
        options.add_argument('--start-maximized')  # Windows용
        options.add_argument('--window-size=1920,1080')  # 최소 해상도 보장
        
        # 기본 설정
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # 다운로드 경로 설정
        prefs = {
            "download.default_directory": self.download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "safebrowsing_for_trusted_sources_enabled": False
        }
        options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=options)
        
        # 추가로 창 최대화 (더블 체크)
        self.driver.maximize_window()
    
    def parse_date(self, date_str):
        """
        날짜 문자열을 파싱하는 보조 함수
        25/07/22 -> 2025-07-22
        
        Parameters:
        -----------
        date_str : str
            날짜 문자열
            
        Returns:
        --------
        datetime
        """
        if pd.isna(date_str):
            return date_str
        
        if isinstance(date_str, str):
            # 25/07/22 형식 처리
            parts = date_str.split('/')
            if len(parts) == 3:
                year = int(parts[0])
                # 2000년대로 가정 (00-99 -> 2000-2099)
                if year < 100:
                    year += 2000
                month = int(parts[1])
                day = int(parts[2])
                return pd.Timestamp(year=year, month=month, day=day)
        
        return pd.to_datetime(date_str)
    
    def wait_for_download(self, timeout=30):
        """
        다운로드 완료 대기
        
        Parameters:
        -----------
        timeout : int
            최대 대기 시간 (초)
        """
        seconds = 0
        while seconds < timeout:
            time.sleep(1)
            # .crdownload 파일이 없으면 다운로드 완료
            temp_files = glob.glob(os.path.join(self.download_path, "*.crdownload"))
            if not temp_files:
                break
            seconds += 1
        
        if seconds >= timeout:
            print(f"다운로드 타임아웃 ({timeout}초)")
        else:
            print(f"다운로드 완료 ({seconds}초 소요)")
    
    def get_latest_excel_file(self, pattern="KMB_파생금리_일자별*.xls"):
        """
        가장 최근에 다운로드된 엑셀 파일 찾기
        
        Parameters:
        -----------
        pattern : str
            파일 패턴
            
        Returns:
        --------
        str or None
            파일 경로
        """
        files = glob.glob(os.path.join(self.download_path, pattern))
        if not files:
            return None
        
        # 가장 최근 파일 반환
        latest_file = max(files, key=os.path.getctime)
        return latest_file
    
    def download_and_read(self, rate_type='IRS'):
        """
        금리 데이터 다운로드 후 DataFrame으로 읽기
        
        Parameters:
        -----------
        rate_type : str
            'IRS' 또는 'CRS'
            
        Returns:
        --------
        pd.DataFrame or None
        """
        try:
            print(f"\n{'='*50}")
            print(f"{rate_type} 데이터 다운로드 시작")
            print('='*50)
            
            # 기존 파일 목록 저장
            existing_files = set(glob.glob(os.path.join(self.download_path, "KMB_파생금리_일자별*.xls")))
            
            # URL 접속
            print(f"1. URL 접속: {self.base_url}")
            self.driver.get(self.base_url)
            wait = WebDriverWait(self.driver, 10)
            
            # 페이지 로딩 대기
            time.sleep(3)
            
            # rate_type에 따라 버튼 클릭
            if rate_type == 'IRS':
                button_xpath = '/html/body/main/article[1]/form/nav/button[1]'
            else:  # CRS
                button_xpath = '/html/body/main/article[1]/form/nav/button[2]'
            
            # 버튼이 보이도록 스크롤 (필요시)
            try:
                button = self.driver.find_element(By.XPATH, button_xpath)
                self.driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(1)
            except:
                pass
            
            # 버튼 클릭
            button = wait.until(EC.element_to_be_clickable((By.XPATH, button_xpath)))
            self.driver.execute_script("arguments[0].click();", button)  # JavaScript로 클릭
            print(f"2. {rate_type} 버튼 클릭 완료")
            
            # 로딩 대기
            time.sleep(3)
            
            # 페이지 하단으로 스크롤
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            print("3. 페이지 스크롤 완료")
            
            # 엑셀 다운로드 버튼 찾기 및 클릭
            excel_button_xpath = '//*[@id="article1"]/footer/button'
            
            # 버튼이 보이도록 스크롤
            try:
                excel_button = self.driver.find_element(By.XPATH, excel_button_xpath)
                self.driver.execute_script("arguments[0].scrollIntoView(true);", excel_button)
                time.sleep(1)
                
                # 버튼이 화면에 보이는지 확인
                print(f"   엑셀 버튼 위치: {excel_button.location}")
                print(f"   엑셀 버튼 크기: {excel_button.size}")
                print(f"   엑셀 버튼 텍스트: {excel_button.text}")
            except Exception as e:
                print(f"   엑셀 버튼 찾기 실패: {e}")
            
            # 엑셀 다운로드 버튼 클릭 (여러 방법 시도)
            excel_button = wait.until(EC.presence_of_element_located((By.XPATH, excel_button_xpath)))
            
            try:
                # 방법 1: 일반 클릭
                excel_button.click()
                print("4. 엑셀 다운로드 버튼 클릭 (일반 클릭)")
            except:
                try:
                    # 방법 2: JavaScript 클릭
                    self.driver.execute_script("arguments[0].click();", excel_button)
                    print("4. 엑셀 다운로드 버튼 클릭 (JavaScript)")
                except:
                    # 방법 3: ActionChains 사용
                    from selenium.webdriver.common.action_chains import ActionChains
                    actions = ActionChains(self.driver)
                    actions.move_to_element(excel_button).click().perform()
                    print("4. 엑셀 다운로드 버튼 클릭 (ActionChains)")
            
            # 다운로드 완료 대기
            print("5. 다운로드 대기 중...")
            self.wait_for_download()
            
            # 새로 다운로드된 파일 찾기
            current_files = set(glob.glob(os.path.join(self.download_path, "KMB_파생금리_일자별*.xls")))
            new_files = current_files - existing_files
            
            if new_files:
                downloaded_file = list(new_files)[0]
                print(f"6. 파일 다운로드 완료: {os.path.basename(downloaded_file)}")
                
                # 파일 목록에 추가 (나중에 삭제용)
                self.downloaded_files.append(downloaded_file)
                
                # DataFrame으로 읽기
                print("7. Excel 파일을 DataFrame으로 변환 중...")
                df = pd.read_excel(downloaded_file)
                
                # 날짜 형식 수정 (25/07/22 -> 2025-07-22)
                # 첫 번째 컬럼이 날짜라고 가정
                if len(df.columns) > 0:
                    date_col = df.columns[0]
                    
                    # 날짜 컬럼이 string 형태인 경우
                    if df[date_col].dtype == 'object':
                        try:
                            # YY/MM/DD 형식으로 파싱
                            # 25/07/22 -> 2025-07-22
                            df[date_col] = pd.to_datetime(df[date_col], format='%y/%m/%d')
                            print(f"   날짜 형식 변환 완료 (YY/MM/DD -> YYYY-MM-DD)")
                        except:
                            try:
                                # 다른 형식 시도 (만약 위 방법이 실패하면)
                                df[date_col] = df[date_col].apply(lambda x: self.parse_date(x))
                                print(f"   날짜 형식 변환 완료 (커스텀 파싱)")
                            except:
                                print(f"   주의: 날짜 형식 자동 변환 실패. 수동 변환 필요")
                
                print(f"8. DataFrame 생성 완료!")
                print(f"   - Shape: {df.shape[0]}행 x {df.shape[1]}열")
                print(f"   - 컬럼: {df.columns.tolist()}")
                
                # 미리보기
                print(f"\n[{rate_type} 데이터 미리보기]")
                print(df.head())
                
                return df
            else:
                print("ERROR: 새로운 파일이 다운로드되지 않았습니다.")
                return None
                
        except Exception as e:
            print(f"ERROR: {rate_type} 데이터 처리 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_both_rates(self):
        """
        IRS와 CRS 데이터를 모두 가져오기
        
        Returns:
        --------
        dict
            {'IRS': DataFrame, 'CRS': DataFrame}
        """
        results = {}
        
        # IRS 데이터 가져오기
        df_irs = self.download_and_read('IRS')
        if df_irs is not None:
            results['IRS'] = df_irs
        
        time.sleep(2)  # 서버 부하 방지
        
        # CRS 데이터 가져오기
        df_crs = self.download_and_read('CRS')
        if df_crs is not None:
            results['CRS'] = df_crs
        
        return results
    
    def cleanup_files(self):
        """
        다운로드된 파일들 삭제
        """
        print(f"\n{'='*50}")
        print("다운로드된 파일 정리")
        print('='*50)
        
        if not self.downloaded_files:
            print("삭제할 파일이 없습니다.")
            return
        
        for file_path in self.downloaded_files:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"삭제 완료: {os.path.basename(file_path)}")
                except Exception as e:
                    print(f"삭제 실패: {os.path.basename(file_path)} - {e}")
            else:
                print(f"파일이 존재하지 않음: {os.path.basename(file_path)}")
        
        self.downloaded_files.clear()
        print("파일 정리 완료!")
    
    def close(self):
        """드라이버 종료"""
        if self.driver:
            self.driver.quit()
            print("\n브라우저 종료")

# 사용 예제
if __name__ == "__main__":
    # 다운로드 경로 설정 (본인 경로로 수정)
    DOWNLOAD_PATH = "C:\\Users\\jesst\\Downloads"  # 여기를 본인 경로로 수정하세요
    
    # 크롤러 초기화
    crawler = KMBRateCrawler(download_path=DOWNLOAD_PATH, headless=False)
    
    try:
        # 방법 1: 개별적으로 다운로드
        print("\n" + "="*60)
        print("KMB 파생금리 데이터 수집 시작")
        print("="*60)
        
        # IRS 데이터
        df_irs = crawler.download_and_read('IRS')
        
        # 잠시 대기
        time.sleep(2)
        
        # CRS 데이터
        df_crs = crawler.download_and_read('CRS')
        
        # 방법 2: 한번에 모두 다운로드
        # all_data = crawler.get_both_rates()
        # df_irs = all_data.get('IRS')
        # df_crs = all_data.get('CRS')
        
        # 데이터 분석 예제
        if df_irs is not None and df_crs is not None:
            print(f"\n{'='*60}")
            print("데이터 수집 완료 - 요약")
            print("="*60)
            
            print(f"\n[IRS 데이터]")
            print(f"  - 데이터 크기: {df_irs.shape}")
            print(f"  - 기간: {df_irs.iloc[0, 0]} ~ {df_irs.iloc[-1, 0]}" if len(df_irs) > 0 else "데이터 없음")
            
            print(f"\n[CRS 데이터]")
            print(f"  - 데이터 크기: {df_crs.shape}")
            print(f"  - 기간: {df_crs.iloc[0, 0]} ~ {df_crs.iloc[-1, 0]}" if len(df_crs) > 0 else "데이터 없음")
            
            # 필요시 데이터 저장 (CSV로)
            # df_irs.to_csv('irs_data.csv', index=False, encoding='utf-8-sig')
            # df_crs.to_csv('crs_data.csv', index=False, encoding='utf-8-sig')
            
            # 데이터 처리 예제
            print(f"\n{'='*60}")
            print("데이터 처리 예제")
            print("="*60)
            
            # 날짜 컬럼을 datetime으로 변환 (첫 번째 컬럼이 날짜라고 가정)
            if len(df_irs.columns) > 0:
                try:
                    date_col = df_irs.columns[0]
                    
                    # IRS 날짜 변환
                    if df_irs[date_col].dtype == 'object':
                        df_irs[date_col] = pd.to_datetime(df_irs[date_col], format='%y/%m/%d')
                    
                    # CRS 날짜 변환  
                    if df_crs[date_col].dtype == 'object':
                        df_crs[date_col] = pd.to_datetime(df_crs[date_col], format='%y/%m/%d')
                    
                    print(f"날짜 컬럼 '{date_col}' 변환 완료 (YY/MM/DD -> YYYY-MM-DD)")
                    
                    # 최근 5일 데이터만 추출
                    recent_irs = df_irs.head(5)
                    recent_crs = df_crs.head(5)
                    
                    print("\n[최근 5일 IRS 데이터]")
                    print(recent_irs)
                    
                    print("\n[최근 5일 CRS 데이터]")
                    print(recent_crs)
                    
                except Exception as e:
                    print(f"날짜 변환 중 오류: {e}")
        
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단되었습니다.")
    
    except Exception as e:
        print(f"\n\n예상치 못한 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 다운로드된 파일 삭제
        crawler.cleanup_files()
        
        # 브라우저 종료
        crawler.close()
        
        print(f"\n{'='*60}")
        print("프로그램 종료")
        print("="*60)
        




IRS=df_irs
CRS= df_crs




import os
import inspect

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


path = r"C:\Users\jesst\Agora\FX\FX_automation.xlsx"
import pandas as pd

IRS['전송일'] = pd.to_datetime(IRS['전송일'], format='%y/%m/%d').dt.strftime("%Y-%m-%d")
CRS['전송일'] = pd.to_datetime(CRS['전송일'], format='%y/%m/%d').dt.strftime("%Y-%m-%d")
print(IRS)
print(CRS)
with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    IRS.to_excel(writer, sheet_name="IRS", index=False)
with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
    CRS.to_excel(writer, sheet_name="CRS", index=False)
