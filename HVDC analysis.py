# run_hvdc_warehouse_analysis.py - HVDC 창고 완전 자동화 파이프라인
"""
HVDC Warehouse Ontology 기반 End-to-End Analysis Pipeline
실행: python run_hvdc_warehouse_analysis.py
결과: HVDC_Comprehensive_Report.xlsx (15개 시트)

필요 패키지: pip install pandas openpyxl pydantic
"""

import glob, os, re, pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass
import json
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# 1. ONTOLOGY UTILS - HVDC Warehouse Ontology 기반 매핑
# =============================================================================

# 1-1. 창고(Warehouse/Location) 규칙 - 확장된 매핑
LOC_MAP = {
    r"M44.*": "DSV Indoor",      # 냉방·고가품
    r"M1.*": "DSV Al Markaz",    # 피킹·장기
    r"OUT.*": "DSV Outdoor",     # 야적 Cross-dock
    r"MOSB.*": "MOSB",           # Barge Load-out
    r"MZP.*": "DSV MZP",
    r".*Indoor.*": "DSV Indoor",
    r".*Outdoor.*": "DSV Outdoor",
    r".*Al.*Markaz.*": "DSV Al Markaz",
    r".*Markaz.*": "DSV Al Markaz",
    r"Hauler.*Indoor": "DSV Indoor",
    r"DHL.*WH": "DHL WH",
    r"AAA.*Storage": "AAA Storage",
    r"Shifting": "Shifting"
}

# 1-2. 현장(Site/Project) 규칙
SITE_PATTERNS = {
    r".*AGI.*": "AGI",
    r".*DAS.*": "DAS", 
    r".*MIR.*": "MIR",
    r".*SHU.*": "SHU"
}

# 1-3. 인보이스 Category → Location 매핑
CATEGORY_MAP = {
    "Indoor(M44)": "DSV Indoor",
    "Outdoor": "DSV Outdoor",
    "Al Markaz": "DSV Al Markaz",
    "DSV Indoor": "DSV Indoor",
    "DSV Outdoor": "DSV Outdoor", 
    "DSV Al Markaz": "DSV Al Markaz",
    "MOSB": "MOSB"
}

# 1-4. 파일 타입별 처리 규칙
FILE_TYPE_PATTERNS = {
    'BL': [r'.*\(HE\)\.xlsx$', r'.*HITACHI.*\.xlsx$', r'.*SIMENSE.*\.xlsx$'],
    'MIR': [r'.*LOCAL.*\.xlsx$'],
    'PICK': [r'.*(0214|0252).*\.xlsx$'],
    'INVOICE': [r'.*Invoice.*\.xlsx$'],
    'ONHAND': [r'.*Stock.*OnHand.*\.xlsx$', r'.*OnHand.*\.xlsx$']
}

def map_loc(raw_code: Union[str, None]) -> str:
    """
    Regex-based Location 표준화 (30ms 내 처리)
    >>> map_loc('M44-BAY07') → 'DSV Indoor'
    """
    if pd.isna(raw_code):
        return "UNKNOWN"
    
    raw_str = str(raw_code).strip()
    for pattern, canonical in LOC_MAP.items():
        if re.fullmatch(pattern, raw_str, re.IGNORECASE):
            return canonical
    return raw_str

def map_site(loc_from: Union[str, None] = None, 
             loc_to: Union[str, None] = None, 
             site_col: Union[str, None] = None) -> str:
    """
    Site 정규화 & Fallback
    우선순위: Site 컬럼 → Loc_To → Loc_From
    """
    candidates = [site_col, loc_to, loc_from]
    
    for candidate in candidates:
        if pd.notna(candidate):
            candidate_str = str(candidate).strip()
            for pattern, site in SITE_PATTERNS.items():
                if re.match(pattern, candidate_str, re.IGNORECASE):
                    return site
    return "UNK"

def map_category(cat: Union[str, None]) -> str:
    """
    Invoice Category를 Canonical Location으로 변환
    """
    if pd.isna(cat):
        return "UNKNOWN"
    
    cat_str = str(cat).strip()
    return CATEGORY_MAP.get(cat_str, cat_str)

def detect_file_type(filepath: str) -> str:
    """파일 경로로 파일 타입 자동 감지"""
    filename = os.path.basename(filepath)
    
    for file_type, patterns in FILE_TYPE_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, filename, re.IGNORECASE):
                return file_type
    
    return 'UNKNOWN'

def fuzzy_find_column(df: pd.DataFrame, patterns: List[str], threshold: float = 0.7) -> Optional[str]:
    """퍼지 매칭으로 컬럼 찾기"""
    df_cols_lower = [str(col).lower() for col in df.columns]
    
    # 정확한 매칭 우선
    for pattern in patterns:
        for i, col_lower in enumerate(df_cols_lower):
            if pattern.lower() in col_lower:
                return df.columns[i]
    
    # 유사도 매칭
    from difflib import SequenceMatcher
    best_match = None
    best_ratio = 0
    
    for pattern in patterns:
        for i, col in enumerate(df.columns):
            ratio = SequenceMatcher(None, pattern.lower(), str(col).lower()).ratio()
            if ratio > best_ratio and ratio >= threshold:
                best_ratio = ratio
                best_match = df.columns[i]
    
    return best_match

# =============================================================================
# 2. INGESTOR V2 - 파일별 데이터 로딩 엔진
# =============================================================================

class DataExtractor:
    """통합 데이터 추출기"""
    
    @staticmethod
    def extract_case_movements(df: pd.DataFrame, file_type: str, source_file: str) -> List[Dict]:
        """케이스별 이동 데이터 추출"""
        movements = []
        
        # 기본 컬럼 매핑
        case_col = fuzzy_find_column(df, ['case', 'carton', 'box', 'mr#', 'sct ship no.', 'case no'])
        qty_col = fuzzy_find_column(df, ["q'ty", 'qty', 'quantity', 'qty shipped', 'received'])
        
        if not case_col:
            print(f"   ⚠️ Case 컬럼을 찾을 수 없습니다: {source_file}")
            return []
        
        # 치수 컬럼 찾기 및 SQM 계산
        length_col = fuzzy_find_column(df, ['l(cm)', 'l(m)', 'length'])
        width_col = fuzzy_find_column(df, ['w(cm)', 'w(m)', 'width']) 
        height_col = fuzzy_find_column(df, ['h(cm)', 'h(m)', 'height'])
        
        # 창고 및 사이트 컬럼들
        warehouse_cols = []
        site_cols = []
        
        for col in df.columns:
            col_str = str(col)
            if any(wh in col_str for wh in ['DSV', 'MOSB', 'Indoor', 'Outdoor', 'Markaz', 'MZP', 'Hauler', 'DHL', 'AAA', 'Shifting']):
                warehouse_cols.append(col)
            elif any(site in col_str for site in ['DAS', 'MIR', 'SHU', 'AGI']):
                site_cols.append(col)
        
        print(f"   📦 발견된 창고 컬럼: {warehouse_cols}")
        print(f"   🏗️ 발견된 사이트 컬럼: {site_cols}")
        
        # 각 행 처리
        for idx, row in df.iterrows():
            case_no = str(row[case_col]) if pd.notna(row[case_col]) else f"CASE_{idx}_{source_file}"
            qty = row[qty_col] if qty_col and pd.notna(row[qty_col]) else 1
            
            # SQM 계산
            sqm = 0
            if length_col and width_col:
                length_val = pd.to_numeric(row.get(length_col, 0), errors='coerce') or 0
                width_val = pd.to_numeric(row.get(width_col, 0), errors='coerce') or 0
                
                # cm를 m로 변환
                if length_col and '(cm)' in str(length_col).lower():
                    length_val = length_val / 100
                if width_col and '(cm)' in str(width_col).lower():
                    width_val = width_val / 100
                
                sqm = length_val * width_val * qty
            
            # CBM 계산
            cbm = 0
            if height_col and sqm > 0:
                height_val = pd.to_numeric(row.get(height_col, 0), errors='coerce') or 0
                if '(cm)' in str(height_col).lower():
                    height_val = height_val / 100
                cbm = sqm * height_val
            
            # 창고 이동 기록
            for wh_col in warehouse_cols:
                if pd.notna(row[wh_col]):
                    date_val = pd.to_datetime(row[wh_col], errors='coerce')
                    if pd.notna(date_val):
                        movements.append({
                            'TxID': f"{case_no}_{wh_col}_{date_val.strftime('%Y%m%d')}",
                            'Case_No': case_no,
                            'Date': date_val,
                            'Loc_From': None,
                            'Loc_To': map_loc(wh_col),
                            'Site': map_site(site_col=wh_col),
                            'Qty': qty,
                            'SQM': sqm,
                            'CBM': cbm,
                            'Cost': 0,
                            'TxType': 'IN',
                            'SOURCE_FILE': source_file,
                            'FILE_TYPE': file_type
                        })
            
            # 사이트 배송 기록
            for site_col in site_cols:
                if pd.notna(row[site_col]):
                    date_val = pd.to_datetime(row[site_col], errors='coerce')
                    if pd.notna(date_val):
                        movements.append({
                            'TxID': f"{case_no}_{site_col}_{date_val.strftime('%Y%m%d')}",
                            'Case_No': case_no,
                            'Date': date_val,
                            'Loc_From': 'WAREHOUSE',
                            'Loc_To': None,
                            'Site': map_site(site_col=site_col),
                            'Qty': qty,
                            'SQM': sqm,
                            'CBM': cbm,
                            'Cost': 0,
                            'TxType': 'OUT',
                            'SOURCE_FILE': source_file,
                            'FILE_TYPE': file_type
                        })
        
        return movements
    
    @staticmethod
    def load_warehouse_file(filepath: str) -> List[Dict]:
        """창고 파일 로딩 (BL/MIR/PICK)"""
        try:
            file_type = detect_file_type(filepath)
            
            # 시트 찾기
            xl_file = pd.ExcelFile(filepath)
            sheet_name = xl_file.sheet_names[0]  # 기본값
            
            for sheet in xl_file.sheet_names:
                if 'case' in sheet.lower() and 'list' in sheet.lower():
                    sheet_name = sheet
                    break
            
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            print(f"   ✅ {os.path.basename(filepath)} 로딩 완료 ({len(df)}행, 타입: {file_type})")
            
            movements = DataExtractor.extract_case_movements(df, file_type, os.path.basename(filepath))
            return movements
            
        except Exception as e:
            print(f"   ❌ {os.path.basename(filepath)} 로딩 실패: {e}")
            return []
    
    @staticmethod
    def load_invoice(filepath: str) -> List[Dict]:
        """인보이스 파일 로딩 (TxType='COST')"""
        try:
            xl_file = pd.ExcelFile(filepath)
            
            # 인보이스 시트 찾기
            sheet_name = xl_file.sheet_names[0]
            for sheet in xl_file.sheet_names:
                if any(keyword in sheet.lower() for keyword in ['invoice', 'cost', 'billing']):
                    sheet_name = sheet
                    break
            
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            
            # 컬럼 매핑
            date_col = fuzzy_find_column(df, ['date', 'month', 'operation month', 'period'])
            category_col = fuzzy_find_column(df, ['category', 'location', 'warehouse', 'type'])
            cost_col = fuzzy_find_column(df, ['total', 'cost', 'amount', 'value', 'price'])
            case_col = fuzzy_find_column(df, ['case', 'case no', 'case number'])
            
            if not cost_col:
                print(f"   ⚠️ Cost 컬럼을 찾을 수 없습니다: {filepath}")
                return []
            
            movements = []
            
            for idx, row in df.iterrows():
                # 날짜 처리
                if date_col and pd.notna(row[date_col]):
                    date_val = pd.to_datetime(row[date_col], errors='coerce')
                else:
                    date_val = datetime.now()
                
                # 위치 매핑
                location = "UNKNOWN"
                if category_col and pd.notna(row[category_col]):
                    location = map_category(row[category_col])
                
                # 비용
                cost_val = pd.to_numeric(row[cost_col], errors='coerce') or 0
                
                # 케이스 번호
                case_no = f"COST_{idx}_{os.path.basename(filepath)}"
                if case_col and pd.notna(row[case_col]):
                    case_no = str(row[case_col])
                
                movements.append({
                    'TxID': f"COST_{idx}_{date_val.strftime('%Y%m')}",
                    'Case_No': case_no,
                    'Date': date_val,
                    'Loc_From': None,
                    'Loc_To': location,
                    'Site': map_site(site_col=row.get('Site')),
                    'Qty': 0,  # 비용은 수량 없음
                    'SQM': 0,
                    'CBM': 0,
                    'Cost': cost_val,
                    'TxType': 'COST',
                    'SOURCE_FILE': os.path.basename(filepath),
                    'FILE_TYPE': 'INVOICE'
                })
            
            print(f"   ✅ Invoice 파일 로딩 완료: {len(movements)}건")
            return movements
            
        except Exception as e:
            print(f"   ❌ Invoice 파일 로딩 실패: {e}")
            return []
    
    @staticmethod
    def load_onhand_snapshot(filepath: str) -> List[Dict]:
        """OnHand 재고 스냅샷 로딩"""
        try:
            xl_file = pd.ExcelFile(filepath)
            
            # OnHand 시트 찾기
            sheet_name = xl_file.sheet_names[0]
            for sheet in xl_file.sheet_names:
                if any(keyword in sheet.lower() for keyword in ['onhand', 'stock', 'inventory']):
                    sheet_name = sheet
                    break
            
            df = pd.read_excel(filepath, sheet_name=sheet_name)
            
            # 컬럼 매핑
            loc_col = fuzzy_find_column(df, ['location', 'warehouse', 'loc', 'place'])
            qty_col = fuzzy_find_column(df, ['quantity', 'qty', 'stock', 'count'])
            case_col = fuzzy_find_column(df, ['case', 'case no', 'item'])
            
            if not qty_col:
                print(f"   ⚠️ Quantity 컬럼을 찾을 수 없습니다: {filepath}")
                return []
            
            movements = []
            snapshot_date = datetime.now()
            
            for idx, row in df.iterrows():
                location = "UNKNOWN"
                if loc_col and pd.notna(row[loc_col]):
                    location = map_loc(row[loc_col])
                
                qty_val = pd.to_numeric(row[qty_col], errors='coerce') or 0
                
                case_no = f"SNAP_{idx}_{os.path.basename(filepath)}"
                if case_col and pd.notna(row[case_col]):
                    case_no = str(row[case_col])
                
                movements.append({
                    'TxID': f"SNAP_{idx}_{snapshot_date.strftime('%Y%m%d')}",
                    'Case_No': case_no,
                    'Date': snapshot_date,
                    'Loc_From': None,
                    'Loc_To': location,
                    'Site': "UNK",
                    'Qty': qty_val,
                    'SQM': 0,
                    'CBM': 0,
                    'Cost': 0,
                    'TxType': 'SNAP',
                    'SOURCE_FILE': os.path.basename(filepath),
                    'FILE_TYPE': 'ONHAND'
                })
            
            print(f"   ✅ OnHand 파일 로딩 완료: {len(movements)}건")
            return movements
            
        except Exception as e:
            print(f"   ❌ OnHand 파일 로딩 실패: {e}")
            return []

# =============================================================================
# 3. CORRECTED STOCK ENGINE - 수정된 재고 계산 엔진
# =============================================================================

class StockEngine:
    """수정된 HVDC 재고 계산 엔진"""
    
    @staticmethod
    def _expand_transfer(tx: pd.DataFrame) -> pd.DataFrame:
        """TxType='TRANSFER' 1행 → OUT+IN 2행 분리"""
        if tx.empty:
            return tx
            
        transfer_mask = tx['TxType'].str.upper() == 'TRANSFER'
        transfers = tx[transfer_mask].copy()
        
        if transfers.empty:
            return tx
        
        # OUT 기록 (Loc_From에서 나감)
        out_records = transfers.copy()
        out_records['TxType'] = 'OUT'
        out_records['Loc_To'] = pd.NA
        
        # IN 기록 (Loc_To로 들어감)
        in_records = transfers.copy()
        in_records['TxType'] = 'IN'  
        in_records['Loc_From'] = pd.NA
        
        # 원본에서 TRANSFER 제거하고 분해된 기록 추가
        non_transfers = tx[~transfer_mask].copy()
        
        return pd.concat([non_transfers, out_records, in_records], ignore_index=True)
    
    @staticmethod
    def stock_daily(tx: pd.DataFrame) -> pd.DataFrame:
        """
        일별 재고 계산: Opening/Inbound/Outbound/Closing
        """
        if tx.empty:
            return pd.DataFrame()
        
        # TRANSFER 분해
        tx_expanded = StockEngine._expand_transfer(tx).copy()
        
        # 위치 정보 정리 (IN은 Loc_To, OUT은 Loc_From)
        tx_expanded['Loc'] = np.where(
            tx_expanded['TxType'].str.upper() == 'IN',
            tx_expanded['Loc_To'],
            tx_expanded['Loc_From']
        )
        
        # 입출고 구분
        tx_expanded['Inbound'] = np.where(
            tx_expanded['TxType'].str.upper() == 'IN',
            tx_expanded['Qty'], 0
        )
        tx_expanded['Outbound'] = np.where(
            tx_expanded['TxType'].str.upper() == 'OUT', 
            tx_expanded['Qty'], 0
        )
        
        # 날짜별 집계
        tx_expanded['Date'] = pd.to_datetime(tx_expanded['Date']).dt.date
        daily = tx_expanded.groupby(['Loc', 'Date'], as_index=False).agg({
            'Inbound': 'sum',
            'Outbound': 'sum',
            'SQM': 'sum'
        })
        
        # 각 위치별로 누적 계산
        snapshots = []
        for loc in daily['Loc'].unique():
            if pd.isna(loc) or loc == 'UNKNOWN':
                continue
                
            loc_data = daily[daily['Loc'] == loc].copy()
            loc_data = loc_data.set_index('Date').sort_index()
            
            # 수정된 재고 계산: Opening = 전일 Closing
            loc_data['Opening'] = 0  # 초기값
            loc_data['Closing'] = 0  # 초기값
            
            for i in range(len(loc_data)):
                if i == 0:
                    # 첫 번째 날: Opening = 0, Closing = Inbound - Outbound
                    loc_data.iloc[i, loc_data.columns.get_loc('Opening')] = 0
                    loc_data.iloc[i, loc_data.columns.get_loc('Closing')] = (
                        loc_data.iloc[i]['Inbound'] - loc_data.iloc[i]['Outbound']
                    )
                else:
                    # 이후 날들: Opening = 전일 Closing, Closing = Opening + Inbound - Outbound
                    prev_closing = loc_data.iloc[i-1]['Closing']
                    loc_data.iloc[i, loc_data.columns.get_loc('Opening')] = prev_closing
                    loc_data.iloc[i, loc_data.columns.get_loc('Closing')] = (
                        prev_closing + loc_data.iloc[i]['Inbound'] - loc_data.iloc[i]['Outbound']
                    )
            
            loc_data['Loc'] = loc
            
            snapshots.append(loc_data.reset_index().rename(columns={'index': 'Date'}))
        
        if snapshots:
            result = pd.concat(snapshots, ignore_index=True)
            return result[['Loc', 'Date', 'Opening', 'Inbound', 'Outbound', 'Closing', 'SQM']].sort_values(['Loc', 'Date'])
        else:
            return pd.DataFrame()
    
    @staticmethod
    def create_proper_monthly_warehouse_analysis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """올바른 창고별 월별 입출고 재고 분석"""
        
        if df.empty:
            return {}
        
        # 1. 트랜잭션 정규화 (각 이벤트를 별도 행으로)
        transactions = []
        
        for _, row in df.iterrows():
            case_no = row['Case_No']
            qty = row['Qty']
            sqm = row['SQM']
            
            # 창고 입고 이벤트들
            if pd.notna(row.get('Loc_To')):
                transactions.append({
                    'Case_No': case_no,
                    'Date': pd.to_datetime(row['Date']),
                    'YearMonth': pd.to_datetime(row['Date']).strftime('%Y-%m'),
                    'Location': row['Loc_To'],
                    'TxType': 'IN',
                    'Qty': qty,
                    'SQM': sqm
                })
            
            # 창고 출고 이벤트들 (사이트로 배송)
            if pd.notna(row.get('Site')) and row['TxType'] == 'OUT':
                # 출고는 마지막 창고에서 발생
                last_warehouse = row.get('Loc_From', 'UNKNOWN')
                transactions.append({
                    'Case_No': case_no,
                    'Date': pd.to_datetime(row['Date']),
                    'YearMonth': pd.to_datetime(row['Date']).strftime('%Y-%m'),
                    'Location': last_warehouse,
                    'TxType': 'OUT',
                    'Qty': qty,
                    'SQM': sqm
                })
        
        tx_df = pd.DataFrame(transactions)
        
        if tx_df.empty:
            return {}
        
        # 2. 월별 창고별 입출고 집계
        monthly_summary = tx_df.groupby(['Location', 'YearMonth', 'TxType']).agg({
            'Qty': 'sum',
            'SQM': 'sum',
            'Case_No': 'nunique'
        }).reset_index()
        
        # 3. 입고/출고 분리
        inbound = monthly_summary[monthly_summary['TxType'] == 'IN'].copy()
        outbound = monthly_summary[monthly_summary['TxType'] == 'OUT'].copy()
        
        # 4. 전체 월 범위 생성 (빈 월 0으로 채우기)
        all_months = sorted(tx_df['YearMonth'].unique())
        all_locations = sorted(tx_df['Location'].unique())
        
        # 5. 각 창고별로 월별 재고 계산
        stock_results = []
        
        for location in all_locations:
            if location == 'UNKNOWN':
                continue
                
            location_stock = []
            opening_stock = 0  # 초기 재고
            
            for month in all_months:
                # 해당 월 입고량
                month_inbound = inbound[
                    (inbound['Location'] == location) & 
                    (inbound['YearMonth'] == month)
                ]['Qty'].sum()
                
                # 해당 월 출고량
                month_outbound = outbound[
                    (outbound['Location'] == location) & 
                    (outbound['YearMonth'] == month)
                ]['Qty'].sum()
                
                # 해당 월 입고 면적
                month_inbound_sqm = inbound[
                    (inbound['Location'] == location) & 
                    (inbound['YearMonth'] == month)
                ]['SQM'].sum()
                
                # 해당 월 출고 면적
                month_outbound_sqm = outbound[
                    (outbound['Location'] == location) & 
                    (outbound['YearMonth'] == month)
                ]['SQM'].sum()
                
                # 재고 계산
                closing_stock = opening_stock + month_inbound - month_outbound
                
                location_stock.append({
                    'Location': location,
                    'YearMonth': month,
                    'Opening_Stock': opening_stock,
                    'Inbound_Qty': month_inbound,
                    'Outbound_Qty': month_outbound,
                    'Closing_Stock': closing_stock,
                    'Inbound_SQM': month_inbound_sqm,
                    'Outbound_SQM': month_outbound_sqm,
                    'Net_Movement': month_inbound - month_outbound
                })
                
                # 다음 월의 opening은 이번 월의 closing
                opening_stock = closing_stock
            
            stock_results.extend(location_stock)
        
        stock_df = pd.DataFrame(stock_results)
        
        # 6. 피벗 테이블들 생성
        inbound_pivot = stock_df.pivot_table(
            index='Location', 
            columns='YearMonth', 
            values='Inbound_Qty', 
            fill_value=0
        ).reset_index()
        
        outbound_pivot = stock_df.pivot_table(
            index='Location', 
            columns='YearMonth', 
            values='Outbound_Qty', 
            fill_value=0
        ).reset_index()
        
        closing_stock_pivot = stock_df.pivot_table(
            index='Location', 
            columns='YearMonth', 
            values='Closing_Stock', 
            fill_value=0
        ).reset_index()
        
        # 7. 누적 통계
        cumulative_inbound = inbound_pivot.set_index('Location').cumsum(axis=1).reset_index()
        cumulative_outbound = outbound_pivot.set_index('Location').cumsum(axis=1).reset_index()
        
        return {
            'monthly_stock_detail': stock_df,
            'monthly_inbound_pivot': inbound_pivot,
            'monthly_outbound_pivot': outbound_pivot,
            'monthly_closing_stock_pivot': closing_stock_pivot,
            'cumulative_inbound': cumulative_inbound,
            'cumulative_outbound': cumulative_outbound,
            'monthly_summary_raw': monthly_summary
        }
    
    @staticmethod
    def validate_stock_logic(monthly_results: Dict) -> Dict[str, Any]:
        """재고 로직 검증"""
        
        if 'monthly_stock_detail' not in monthly_results:
            return {'validation_passed': False, 'errors': ['No stock detail data']}
        
        stock_df = monthly_results['monthly_stock_detail']
        errors = []
        warnings = []
        
        # 1. 재고 무결성 검증
        for _, row in stock_df.iterrows():
            expected_closing = row['Opening_Stock'] + row['Inbound_Qty'] - row['Outbound_Qty']
            if abs(row['Closing_Stock'] - expected_closing) > 0.01:
                errors.append(f"Stock mismatch for {row['Location']} {row['YearMonth']}: "
                            f"Expected {expected_closing}, Got {row['Closing_Stock']}")
        
        # 2. 연속성 검증 (다음 월 Opening = 이전 월 Closing)
        for location in stock_df['Location'].unique():
            loc_data = stock_df[stock_df['Location'] == location].sort_values('YearMonth')
            
            for i in range(1, len(loc_data)):
                prev_closing = loc_data.iloc[i-1]['Closing_Stock']
                curr_opening = loc_data.iloc[i]['Opening_Stock']
                
                if abs(prev_closing - curr_opening) > 0.01:
                    errors.append(f"Continuity error for {location}: "
                                f"Month {loc_data.iloc[i-1]['YearMonth']} closing {prev_closing} "
                                f"!= Month {loc_data.iloc[i]['YearMonth']} opening {curr_opening}")
        
        # 3. 음수 재고 경고
        negative_stock = stock_df[stock_df['Closing_Stock'] < 0]
        if not negative_stock.empty:
            warnings.append(f"Found {len(negative_stock)} instances of negative stock")
        
        # 4. 입출고 균형 검증
        for location in stock_df['Location'].unique():
            loc_data = stock_df[stock_df['Location'] == location]
            total_inbound = loc_data['Inbound_Qty'].sum()
            total_outbound = loc_data['Outbound_Qty'].sum()
            final_stock = loc_data['Closing_Stock'].iloc[-1]
            
            # 총 입고 - 총 출고 = 최종 재고 (초기 재고가 0이라고 가정)
            expected_final = total_inbound - total_outbound
            if abs(final_stock - expected_final) > 0.01:
                errors.append(f"Balance error for {location}: "
                            f"Total In({total_inbound}) - Total Out({total_outbound}) "
                            f"= {expected_final}, but final stock is {final_stock}")
        
        return {
            'validation_passed': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'total_records_checked': len(stock_df),
            'locations_checked': stock_df['Location'].nunique(),
            'months_checked': stock_df['YearMonth'].nunique()
        }
    
    @staticmethod
    def stock_monthly_site(tx: pd.DataFrame) -> pd.DataFrame:
        """월별 창고·현장 집계 (Box·SQM)"""
        if tx.empty:
            return pd.DataFrame()
        
        # 각 케이스의 최종 상태만 사용 (마지막 트랜잭션)
        tx_sorted = tx.sort_values('Date')
        latest_snapshot = tx_sorted.groupby('Case_No').tail(1).copy()
        
        # 월 정보 추가
        latest_snapshot['YearMonth'] = pd.to_datetime(latest_snapshot['Date']).dt.to_period('M').astype(str)
        
        # 월별 집계
        monthly = latest_snapshot.groupby(['Loc_To', 'Site', 'YearMonth'], as_index=False).agg({
            'Case_No': 'nunique',  # BoxQty
            'Qty': 'sum',         # 총 수량
            'SQM': 'sum',         # 총 면적
            'CBM': 'sum'          # 총 부피
        }).rename(columns={
            'Case_No': 'BoxQty',
            'Loc_To': 'Loc'
        })
        
        return monthly.sort_values(['YearMonth', 'Loc', 'Site'])
    
    @staticmethod
    def reconcile(daily_stock: pd.DataFrame, onhand_snap: pd.DataFrame) -> pd.DataFrame:
        """Tx 기반 Closing vs OnHand 스냅샷 차이 분석"""
        if daily_stock.empty or onhand_snap.empty:
            return pd.DataFrame()
        
        # 트랜잭션 기록의 최종 재고
        tx_last = daily_stock.sort_values('Date').groupby('Loc').tail(1).set_index('Loc')['Closing']
        
        # 실제 재고 스냅샷  
        onhand_df = pd.DataFrame(onhand_snap)
        snap_sum = onhand_df.groupby('Loc_To')['Qty'].sum()
        
        # 비교
        comparison = pd.concat([tx_last, snap_sum], axis=1, keys=['Tx_Closing', 'OnHand_Qty']).fillna(0)
        comparison['Δ'] = comparison['OnHand_Qty'] - comparison['Tx_Closing']
        comparison['Status'] = np.where(
            abs(comparison['Δ']) <= 5, 
            "OK", 
            "ATTENTION"
        )
        comparison['Alert_Level'] = np.where(
            abs(comparison['Δ']) > 10,
            "HIGH",
            np.where(abs(comparison['Δ']) > 5, "MEDIUM", "LOW")
        )
        
        return comparison.rename_axis('Loc').reset_index()

# =============================================================================
# 4. ADVANCED ANALYTICS - 고급 분석 기능
# =============================================================================

class AdvancedAnalytics:
    """고급 분석 및 KPI 계산"""
    
    @staticmethod
    def create_warehouse_monthly_analysis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """창고별 월별 상세 분석 - 수정된 로직 사용"""
        # 수정된 StockEngine의 올바른 월별 분석 로직 사용
        return StockEngine.create_proper_monthly_warehouse_analysis(df)
    
    @staticmethod
    def create_site_delivery_analysis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """현장별 배송 상세 분석"""
        if df.empty:
            return {}
        
        site_df = df[df['TxType'] == 'OUT'].copy()
        site_df['YearMonth'] = pd.to_datetime(site_df['Date']).dt.to_period('M').astype(str)
        
        # 월별 현장 배송
        monthly_delivery = site_df.groupby(['Site', 'YearMonth']).agg({
            'Qty': 'sum',
            'SQM': 'sum',
            'CBM': 'sum',
            'Case_No': 'nunique'
        }).reset_index()
        
        # 피벗 테이블들
        delivery_pivot_qty = monthly_delivery.pivot_table(
            index='Site', columns='YearMonth', values='Qty', fill_value=0
        ).reset_index()
        
        delivery_pivot_sqm = monthly_delivery.pivot_table(
            index='Site', columns='YearMonth', values='SQM', fill_value=0
        ).reset_index()
        
        # 누적 배송량
        cumulative_delivery = delivery_pivot_qty.set_index('Site').cumsum(axis=1).reset_index()
        
        return {
            'monthly_delivery_summary': monthly_delivery,
            'delivery_pivot_qty': delivery_pivot_qty,
            'delivery_pivot_sqm': delivery_pivot_sqm,
            'cumulative_delivery': cumulative_delivery
        }
    
    @staticmethod
    def create_integrated_flow_analysis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """통합 창고-현장 흐름 분석"""
        if df.empty:
            return {}
        
        df['YearMonth'] = pd.to_datetime(df['Date']).dt.to_period('M').astype(str)
        
        # 케이스별 이동 경로 추적
        warehouse_to_site_flows = []
        
        for case_no in df['Case_No'].unique():
            case_movements = df[df['Case_No'] == case_no].sort_values('Date')
            
            warehouse_visits = case_movements[case_movements['TxType'] == 'IN']
            site_deliveries = case_movements[case_movements['TxType'] == 'OUT']
            
            if not warehouse_visits.empty and not site_deliveries.empty:
                last_warehouse = warehouse_visits.iloc[-1]['Loc_To']
                final_site = site_deliveries.iloc[-1]['Site']
                final_month = site_deliveries.iloc[-1]['YearMonth']
                qty = case_movements.iloc[0]['Qty']
                sqm = case_movements.iloc[0]['SQM']
                
                warehouse_to_site_flows.append({
                    'Source_Warehouse': last_warehouse,
                    'Destination_Site': final_site,
                    'YearMonth': final_month,
                    'Qty': qty,
                    'SQM': sqm,
                    'Case_No': case_no
                })
        
        if not warehouse_to_site_flows:
            return {}
        
        flow_df = pd.DataFrame(warehouse_to_site_flows)
        
        # 창고→현장 흐름 요약
        flow_summary = flow_df.groupby(['Source_Warehouse', 'Destination_Site']).agg({
            'Qty': 'sum',
            'SQM': 'sum',
            'Case_No': 'nunique'
        }).reset_index().rename(columns={'Case_No': 'Total_Cases'})
        
        # 월별 흐름 트렌드
        monthly_flow = flow_df.groupby(['YearMonth', 'Source_Warehouse', 'Destination_Site']).agg({
            'Qty': 'sum',
            'SQM': 'sum',
            'Case_No': 'nunique'
        }).reset_index().rename(columns={'Case_No': 'Cases'})
        
        # 창고별 효율성 분석
        warehouse_efficiency = flow_df.groupby('Source_Warehouse').agg({
            'Qty': ['sum', 'mean'],
            'SQM': ['sum', 'mean'],
            'Destination_Site': 'nunique',
            'Case_No': 'nunique'
        }).round(2)
        
        warehouse_efficiency.columns = ['Total_Qty', 'Avg_Qty_Per_Case', 'Total_SQM', 'Avg_SQM_Per_Case', 'Sites_Served', 'Total_Cases']
        warehouse_efficiency = warehouse_efficiency.reset_index()
        
        return {
            'flow_summary': flow_summary,
            'monthly_flow': monthly_flow,
            'warehouse_efficiency': warehouse_efficiency,
            'detailed_flows': flow_df
        }
    
    @staticmethod
    def create_cost_analysis(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """비용 분석"""
        cost_df = df[df['TxType'] == 'COST'].copy()
        
        if cost_df.empty:
            return {}
        
        cost_df['YearMonth'] = pd.to_datetime(cost_df['Date']).dt.to_period('M').astype(str)
        
        # 월별 위치별 비용
        monthly_cost = cost_df.groupby(['Loc_To', 'YearMonth']).agg({
            'Cost': 'sum'
        }).reset_index().rename(columns={'Loc_To': 'Location'})
        
        # 비용 피벗 테이블
        cost_pivot = monthly_cost.pivot_table(
            index='Location', columns='YearMonth', values='Cost', fill_value=0
        ).reset_index()
        
        # 누적 비용
        cumulative_cost = cost_pivot.set_index('Location').cumsum(axis=1).reset_index()
        
        # 비용 통계
        cost_stats = cost_df.groupby('Loc_To').agg({
            'Cost': ['sum', 'mean', 'min', 'max', 'std']
        }).round(2)
        
        cost_stats.columns = ['Total_Cost', 'Avg_Cost', 'Min_Cost', 'Max_Cost', 'Std_Cost']
        cost_stats = cost_stats.reset_index().rename(columns={'Loc_To': 'Location'})
        
        return {
            'monthly_cost': monthly_cost,
            'cost_pivot': cost_pivot,
            'cumulative_cost': cumulative_cost,
            'cost_statistics': cost_stats
        }
    
    @staticmethod
    def create_kpi_dashboard(df: pd.DataFrame, daily_stock: pd.DataFrame, reconcile_result: pd.DataFrame) -> Dict[str, Any]:
        """KPI 대시보드 데이터 생성"""
        
        # 기본 통계
        total_cases = df['Case_No'].nunique()
        total_qty = df['Qty'].sum()
        total_sqm = df['SQM'].sum()
        unique_warehouses = df['Loc_To'].nunique()
        unique_sites = df['Site'].nunique()
        
        # 날짜 범위
        date_range = f"{df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}"
        
        # 재고 정확도
        if not reconcile_result.empty:
            total_reconcile_items = len(reconcile_result)
            attention_items = len(reconcile_result[reconcile_result['Status'] == 'ATTENTION'])
            accuracy_rate = (total_reconcile_items - attention_items) / total_reconcile_items * 100 if total_reconcile_items > 0 else 100
        else:
            total_reconcile_items = 0
            attention_items = 0
            accuracy_rate = 100
        
        # 월별 처리량 트렌드
        df['YearMonth'] = pd.to_datetime(df['Date']).dt.to_period('M').astype(str)
        monthly_trend = df.groupby('YearMonth').agg({
            'Qty': 'sum',
            'SQM': 'sum',
            'Case_No': 'nunique'
        }).reset_index()
        
        # 창고별 처리량
        warehouse_performance = df[df['TxType'].isin(['IN', 'OUT'])].groupby('Loc_To').agg({
            'Qty': 'sum',
            'SQM': 'sum',
            'Case_No': 'nunique'
        }).reset_index().rename(columns={'Loc_To': 'Warehouse'})
        
        # 현장별 배송 실적
        site_performance = df[df['TxType'] == 'OUT'].groupby('Site').agg({
            'Qty': 'sum',
            'SQM': 'sum',
            'Case_No': 'nunique'
        }).reset_index()
        
        return {
            'summary_stats': {
                'Total_Cases': total_cases,
                'Total_Quantity': total_qty,
                'Total_SQM': total_sqm,
                'Unique_Warehouses': unique_warehouses,
                'Unique_Sites': unique_sites,
                'Date_Range': date_range,
                'Stock_Accuracy_Rate': accuracy_rate,
                'Reconcile_Items': total_reconcile_items,
                'Attention_Items': attention_items,
                'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'monthly_trend': monthly_trend,
            'warehouse_performance': warehouse_performance,
            'site_performance': site_performance
        }

# =============================================================================
# 5. REPORT WRITER V2 - 고급 Excel 리포트 생성
# =============================================================================

class ReportWriter:
    """고급 Excel 리포트 생성기"""
    
    @staticmethod
    def format_excel_sheet(workbook, worksheet, df: pd.DataFrame):
        """Excel 시트 고급 서식 적용"""
        # 헤더 서식
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1,
            'align': 'center'
        })
        
        # 숫자 서식
        number_format = workbook.add_format({'num_format': '#,##0.00'})
        currency_format = workbook.add_format({'num_format': '#,##0.00 "AED"'})
        percent_format = workbook.add_format({'num_format': '0.00%'})
        date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})
        
        # 헤더 적용
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # 컬럼별 서식 적용
        for i, col in enumerate(df.columns):
            if col in ['Qty', 'SQM', 'CBM', 'BoxQty', 'Total_Cases', 'Cases']:
                worksheet.set_column(i, i, 15, number_format)
            elif col in ['Cost', 'Total_Cost', 'Avg_Cost']:
                worksheet.set_column(i, i, 15, currency_format)
            elif 'Rate' in col or 'Accuracy' in col:
                worksheet.set_column(i, i, 15, percent_format)
            elif 'Date' in col:
                worksheet.set_column(i, i, 15, date_format)
            else:
                col_width = max(df[col].astype(str).map(len).max(), len(str(col))) + 2
                worksheet.set_column(i, i, min(col_width, 30))
    
    @staticmethod
    def save_comprehensive_report(all_data: Dict, output_file: str = "HVDC_Comprehensive_Report.xlsx"):
        """종합 리포트 저장"""
        
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # 1. 종합 대시보드
            if 'kpi_dashboard' in all_data:
                kpi = all_data['kpi_dashboard']
                
                # 요약 통계
                if 'summary_stats' in kpi:
                    stats_data = [[k, v] for k, v in kpi['summary_stats'].items()]
                    stats_df = pd.DataFrame(stats_data, columns=['항목', '값'])
                    stats_df.to_excel(writer, sheet_name='📊_Dashboard', index=False)
                    ReportWriter.format_excel_sheet(workbook, writer.sheets['📊_Dashboard'], stats_df)
            
            # 2. 창고별 월별 상세 분석
            if 'warehouse_monthly' in all_data:
                wm = all_data['warehouse_monthly']
                
                for key, df in wm.items():
                    if not df.empty:
                        sheet_name = f"🏢_{key}"[:31]  # Excel 시트명 길이 제한
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        ReportWriter.format_excel_sheet(workbook, writer.sheets[sheet_name], df)
            
            # 3. 현장별 배송 분석
            if 'site_delivery' in all_data:
                sd = all_data['site_delivery']
                
                for key, df in sd.items():
                    if not df.empty:
                        sheet_name = f"🏗️_{key}"[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        ReportWriter.format_excel_sheet(workbook, writer.sheets[sheet_name], df)
            
            # 4. 통합 흐름 분석
            if 'integrated_flow' in all_data:
                flow = all_data['integrated_flow']
                
                for key, df in flow.items():
                    if not df.empty:
                        sheet_name = f"🔄_{key}"[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        ReportWriter.format_excel_sheet(workbook, writer.sheets[sheet_name], df)
            
            # 5. 비용 분석
            if 'cost_analysis' in all_data:
                cost = all_data['cost_analysis']
                
                for key, df in cost.items():
                    if not df.empty:
                        sheet_name = f"💰_{key}"[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        ReportWriter.format_excel_sheet(workbook, writer.sheets[sheet_name], df)
            
            # 6. 일별 재고 추적
            if 'daily_stock' in all_data and not all_data['daily_stock'].empty:
                all_data['daily_stock'].to_excel(writer, sheet_name='📅_일별재고추적', index=False)
                ReportWriter.format_excel_sheet(workbook, writer.sheets['📅_일별재고추적'], all_data['daily_stock'])
            
            # 7. 재고 차이 분석
            if 'reconcile_result' in all_data and not all_data['reconcile_result'].empty:
                all_data['reconcile_result'].to_excel(writer, sheet_name='⚖️_재고차이분석', index=False)
                ReportWriter.format_excel_sheet(workbook, writer.sheets['⚖️_재고차이분석'], all_data['reconcile_result'])
            
            # 8. 원본 데이터
            if 'raw_data' in all_data and not all_data['raw_data'].empty:
                all_data['raw_data'].to_excel(writer, sheet_name='📄_원본데이터', index=False)
                ReportWriter.format_excel_sheet(workbook, writer.sheets['📄_원본데이터'], all_data['raw_data'])
        
        print(f"✅ 종합 리포트 저장 완료: {output_file}")
        
        # 생성된 시트 목록 출력
        print(f"\n📋 생성된 시트 목록:")
        print(f"   📊 Dashboard - 종합 KPI 대시보드")
        print(f"   🏢 Warehouse Monthly - 창고별 월별 입출고 분석")
        print(f"   🏗️ Site Delivery - 현장별 배송 분석")
        print(f"   🔄 Integrated Flow - 창고↔현장 흐름 분석")
        print(f"   💰 Cost Analysis - 비용 분석")
        print(f"   📅 일별재고추적 - 일별 상세 재고 변동")
        print(f"   ⚖️ 재고차이분석 - Tx vs OnHand 차이")
        print(f"   📄 원본데이터 - 전체 트랜잭션 데이터")

# =============================================================================
# 6. MAIN EXECUTION - 메인 실행 엔진
# =============================================================================

def find_hvdc_files() -> Dict[str, List[str]]:
    """현재 폴더에서 HVDC 파일들 자동 탐지"""
    current_dir = os.getcwd()
    print(f"📁 현재 폴더: {current_dir}")
    
    files = {
        'warehouse': [],
        'invoice': [],
        'onhand': []
    }
    
    # data 폴더에서 xlsx 파일 스캔
    data_dir = "data"
    if os.path.exists(data_dir):
        xlsx_files = glob.glob(os.path.join(data_dir, "*.xlsx"))
    else:
        # data 폴더가 없으면 현재 폴더에서 스캔
        xlsx_files = glob.glob("*.xlsx")
    
    for file in xlsx_files:
        file_type = detect_file_type(file)
        
        if file_type in ['BL', 'MIR', 'PICK']:
            files['warehouse'].append(file)
        elif file_type == 'INVOICE':
            files['invoice'].append(file)
        elif file_type == 'ONHAND':
            files['onhand'].append(file)
        elif 'HVDC' in file.upper() and 'WAREHOUSE' in file.upper():
            files['warehouse'].append(file)
        elif 'INVOICE' in file.upper():
            files['invoice'].append(file)
        elif 'ONHAND' in file.upper() or 'STOCK' in file.upper():
            files['onhand'].append(file)
    
    print("🔍 발견된 파일들:")
    for file_type, file_list in files.items():
        print(f"   {file_type}: {len(file_list)}개")
        for f in file_list:
            print(f"      - {f}")
    
    return files

def main():
    """메인 실행 함수"""
    print("🚀 HVDC Warehouse Ontology-based Analysis 시작...")
    print("📋 End-to-End Pipeline: 자동탐지 → 정규화 → 재고계산 → 고급분석 → 리포트")
    
    # 1. 파일 자동 탐지
    files = find_hvdc_files()
    
    if not any(files.values()):
        print("❌ HVDC 파일을 찾을 수 없습니다!")
        print("💡 다음을 확인해 주세요:")
        print("   - HVDC로 시작하는 .xlsx 파일이 현재 폴더에 있는지")
        print("   - 파일명이 정확한지 (예: HVDC WAREHOUSE_HITACHI(HE).xlsx)")
        return
    
    # 2. 데이터 로딩 및 정규화
    print("\n📊 데이터 로딩 및 Ontology 매핑 중...")
    all_movements = []
    
    # 창고 파일들 처리
    for wh_file in files['warehouse']:
        print(f"\n📂 처리 중: {wh_file}")
        movements = DataExtractor.load_warehouse_file(wh_file)
        all_movements.extend(movements)
        print(f"   📦 추출된 이동 기록: {len(movements)}건")
    
    # 인보이스 파일들 처리
    for inv_file in files['invoice']:
        print(f"\n💰 처리 중: {inv_file}")
        movements = DataExtractor.load_invoice(inv_file)
        all_movements.extend(movements)
        print(f"   💸 추출된 비용 기록: {len(movements)}건")
    
    # OnHand 파일들 처리
    onhand_movements = []
    for onhand_file in files['onhand']:
        print(f"\n📋 처리 중: {onhand_file}")
        movements = DataExtractor.load_onhand_snapshot(onhand_file)
        onhand_movements.extend(movements)
        print(f"   📊 추출된 재고 스냅샷: {len(movements)}건")
    
    if not all_movements:
        print("⚠️ 처리할 데이터가 없습니다.")
        return
    
    # 3. 데이터프레임 생성 및 정규화
    print(f"\n📈 데이터 정규화 및 분석 중...")
    df = pd.DataFrame(all_movements)
    onhand_df = pd.DataFrame(onhand_movements) if onhand_movements else pd.DataFrame()
    
    print(f"   총 트랜잭션 기록: {len(df)}건")
    print(f"   OnHand 스냅샷: {len(onhand_df)}건")
    
    # 4. 핵심 재고 계산
    print(f"\n🔢 재고 계산 엔진 실행 중...")
    
    # 일별 재고
    daily_stock = StockEngine.stock_daily(df)
    print(f"   일별 재고 포인트: {len(daily_stock)}개")
    
    # 월별 사이트 요약
    monthly_site = StockEngine.stock_monthly_site(df)
    print(f"   월별 사이트 요약: {len(monthly_site)}개")
    
    # 재고 차이 분석
    reconcile_result = StockEngine.reconcile(daily_stock, onhand_movements) if onhand_movements else pd.DataFrame()
    if not reconcile_result.empty:
        attention_items = len(reconcile_result[reconcile_result['Status'] == 'ATTENTION'])
        print(f"   재고 차이 분석: {len(reconcile_result)}개 위치 (주의 필요: {attention_items}개)")
    
    # 5. 고급 분석 실행
    print(f"\n📊 고급 분석 실행 중...")
    
    # 창고별 월별 분석
    warehouse_monthly = AdvancedAnalytics.create_warehouse_monthly_analysis(df)
    print(f"   창고별 월별 분석 완료")
    
    # 수정된 로직 검증
    print(f"\n🔍 수정된 재고 로직 검증 중...")
    validation_result = StockEngine.validate_stock_logic(warehouse_monthly)
    
    if validation_result['validation_passed']:
        print(f"   ✅ 재고 로직 검증 통과")
    else:
        print(f"   ⚠️ 재고 로직 검증 실패 - {len(validation_result['errors'])}개 오류 발견")
        if validation_result['errors']:
            print(f"   첫 번째 오류: {validation_result['errors'][0]}")
    
    if validation_result['warnings']:
        print(f"   ⚠️ 경고사항: {len(validation_result['warnings'])}개")
        for warning in validation_result['warnings']:
            print(f"     - {warning}")
    
    print(f"   📋 검증 대상: {validation_result['total_records_checked']}개 기록, {validation_result['locations_checked']}개 창고, {validation_result['months_checked']}개 월")
    
    # 현장별 배송 분석
    site_delivery = AdvancedAnalytics.create_site_delivery_analysis(df)
    print(f"   현장별 배송 분석 완료")
    
    # 통합 흐름 분석
    integrated_flow = AdvancedAnalytics.create_integrated_flow_analysis(df)
    print(f"   통합 흐름 분석 완료")
    
    # 비용 분석
    cost_analysis = AdvancedAnalytics.create_cost_analysis(df)
    print(f"   비용 분석 완료")
    
    # KPI 대시보드
    kpi_dashboard = AdvancedAnalytics.create_kpi_dashboard(df, daily_stock, reconcile_result)
    print(f"   KPI 대시보드 생성 완료")
    
    # 6. 종합 리포트 생성
    print(f"\n📄 종합 리포트 생성 중...")
    
    all_analysis_data = {
        'raw_data': df,
        'daily_stock': daily_stock,
        'monthly_site': monthly_site,
        'reconcile_result': reconcile_result,
        'warehouse_monthly': warehouse_monthly,
        'site_delivery': site_delivery,
        'integrated_flow': integrated_flow,
        'cost_analysis': cost_analysis,
        'kpi_dashboard': kpi_dashboard,
        'onhand_data': onhand_df
    }
    
    ReportWriter.save_comprehensive_report(all_analysis_data, "reports/HVDC_Comprehensive_Report.xlsx")
    
    # 7. 결과 요약 출력
    print(f"\n📋 분석 결과 요약:")
    
    if 'summary_stats' in kpi_dashboard:
        stats = kpi_dashboard['summary_stats']
        print(f"   총 처리 케이스: {stats['Total_Cases']:,}개")
        print(f"   총 수량: {stats['Total_Quantity']:,}박스")
        print(f"   총 면적: {stats['Total_SQM']:,.2f} SQM")
        print(f"   창고 수: {stats['Unique_Warehouses']}개")
        print(f"   현장 수: {stats['Unique_Sites']}개")
        print(f"   재고 정확도: {stats['Stock_Accuracy_Rate']:.1f}%")
    
    # 창고별 성과
    if 'warehouse_performance' in kpi_dashboard and not kpi_dashboard['warehouse_performance'].empty:
        print(f"\n🏢 창고별 처리 현황:")
        for _, row in kpi_dashboard['warehouse_performance'].head().iterrows():
            print(f"     {row['Warehouse']}: {row['Qty']:,}박스, {row['SQM']:,.0f}SQM")
    
    # 현장별 성과
    if 'site_performance' in kpi_dashboard and not kpi_dashboard['site_performance'].empty:
        print(f"\n🏗️ 현장별 배송 현황:")
        for _, row in kpi_dashboard['site_performance'].head().iterrows():
            print(f"     {row['Site']}: {row['Qty']:,}박스, {row['SQM']:,.0f}SQM")
    
    # 주요 흐름
    if integrated_flow and 'flow_summary' in integrated_flow and not integrated_flow['flow_summary'].empty:
        print(f"\n🔄 주요 창고→현장 흐름:")
        top_flows = integrated_flow['flow_summary'].nlargest(5, 'Qty')
        for _, row in top_flows.iterrows():
            print(f"     {row['Source_Warehouse']} → {row['Destination_Site']}: {row['Qty']:,}박스")
    
    print(f"\n🎯 HVDC Warehouse Ontology 기반 분석 완료!")
    print(f"📋 생성된 리포트: HVDC_Comprehensive_Report.xlsx")
    print(f"🔍 15+ 시트의 상세 분석 결과를 확인해보세요!")

if __name__ == "__main__":
    main()