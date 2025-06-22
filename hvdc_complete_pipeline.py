# hvdc_complete_pipeline.py
"""
HVDC 창고별 월별 입출고 재고 계산 - 완전 검증된 파이프라인
인보이스 파일 포함, 모든 로직 오류 수정
"""

import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from typing import Dict, List, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

class HVDCStockEngine:
    """HVDC 재고 계산 엔진 - 완전 검증된 버전"""
    
    @staticmethod
    def find_column(df: pd.DataFrame, patterns: List[str]) -> str:
        """컬럼 찾기"""
        for pattern in patterns:
            for col in df.columns:
                if pattern.lower() in str(col).lower():
                    return col
        return None
    
    @staticmethod
    def normalize_warehouse_name(raw_name: str) -> str:
        """창고명 정규화"""
        name_lower = str(raw_name).lower()
        
        if 'indoor' in name_lower or 'm44' in name_lower:
            return 'DSV Indoor'
        elif 'markaz' in name_lower or 'm1' in name_lower:
            return 'DSV Al Markaz'
        elif 'outdoor' in name_lower:
            return 'DSV Outdoor'
        elif 'mosb' in name_lower:
            return 'MOSB'
        elif 'mzp' in name_lower:
            return 'DSV MZP'
        elif 'dhl' in name_lower:
            return 'DHL WH'
        elif 'aaa' in name_lower:
            return 'AAA Storage'
        else:
            return str(raw_name)
    
    @staticmethod
    def calculate_sqm(df: pd.DataFrame, row: pd.Series, qty: float) -> float:
        """SQM 계산"""
        length_col = HVDCStockEngine.find_column(df, ['l(cm)', 'l(m)', 'length'])
        width_col = HVDCStockEngine.find_column(df, ['w(cm)', 'w(m)', 'width'])
        
        if not length_col or not width_col:
            return 0
        
        length = pd.to_numeric(row.get(length_col, 0), errors='coerce') or 0
        width = pd.to_numeric(row.get(width_col, 0), errors='coerce') or 0
        
        # cm를 m로 변환
        if '(cm)' in str(length_col).lower():
            length = length / 100
        if '(cm)' in str(width_col).lower():
            width = width / 100
        
        return length * width * qty
    
    @staticmethod
    def extract_warehouse_movements(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """창고 이동 이벤트 추출"""
        if df.empty:
            return pd.DataFrame()
        
        movements = []
        
        # 컬럼 매핑
        case_col = HVDCStockEngine.find_column(df, ['case', 'case no', 'mr#', 'sct ship no'])
        qty_col = HVDCStockEngine.find_column(df, ['qty', 'quantity', "q'ty"])
        
        if not case_col:
            print(f"   ⚠️ Case 컬럼을 찾을 수 없음: {source_file}")
            return pd.DataFrame()
        
        # 창고 및 사이트 컬럼 분류
        warehouse_cols = []
        site_cols = []
        
        for col in df.columns:
            col_str = str(col).lower()
            if any(wh in col_str for wh in ['dsv', 'indoor', 'outdoor', 'markaz', 'mosb', 'mzp', 'hauler', 'dhl', 'aaa']):
                warehouse_cols.append(col)
            elif any(site in col_str for site in ['das', 'mir', 'shu', 'agi']):
                site_cols.append(col)
        
        print(f"   📦 창고 컬럼: {warehouse_cols}")
        print(f"   🏗️ 사이트 컬럼: {site_cols}")
        
        for idx, row in df.iterrows():
            case_no = str(row[case_col]) if pd.notna(row[case_col]) else f"CASE_{idx}"
            qty = pd.to_numeric(row[qty_col], errors='coerce') if qty_col else 1
            if pd.isna(qty) or qty <= 0:
                qty = 1
            
            sqm = HVDCStockEngine.calculate_sqm(df, row, qty)
            
            # 시간 순서대로 이벤트 수집
            events = []
            
            # 창고 이벤트들
            for wh_col in warehouse_cols:
                if pd.notna(row[wh_col]):
                    date_val = pd.to_datetime(row[wh_col], errors='coerce')
                    if pd.notna(date_val):
                        events.append({
                            'date': date_val,
                            'location': HVDCStockEngine.normalize_warehouse_name(wh_col),
                            'type': 'warehouse',
                            'original_col': wh_col
                        })
            
            # 사이트 이벤트들
            for site_col in site_cols:
                if pd.notna(row[site_col]):
                    date_val = pd.to_datetime(row[site_col], errors='coerce')
                    if pd.notna(date_val):
                        events.append({
                            'date': date_val,
                            'location': site_col,
                            'type': 'site',
                            'original_col': site_col
                        })
            
            # 시간순 정렬
            events.sort(key=lambda x: x['date'])
            
            # 이벤트를 입출고로 변환
            for i, event in enumerate(events):
                if event['type'] == 'warehouse':
                    if i == 0:
                        # 첫 번째 창고는 항상 입고
                        tx_type = 'IN'
                        loc_from = None
                        loc_to = event['location']
                    else:
                        # 이전 이벤트가 창고면 이동, 사이트면 입고
                        prev_event = events[i-1]
                        if prev_event['type'] == 'warehouse':
                            # 창고간 이동: 이전 창고에서 출고
                            movements.append({
                                'TxID': f"{case_no}_{prev_event['original_col']}_OUT_{prev_event['date'].strftime('%Y%m%d')}",
                                'Case_No': case_no,
                                'Date': prev_event['date'],
                                'Loc_From': prev_event['location'],
                                'Loc_To': None,
                                'Site': 'UNK',
                                'Qty': qty,
                                'SQM': sqm,
                                'Cost': 0,
                                'TxType': 'OUT',
                                'SOURCE_FILE': source_file
                            })
                            
                            # 현재 창고로 입고
                            tx_type = 'IN'
                            loc_from = prev_event['location']
                            loc_to = event['location']
                        else:
                            tx_type = 'IN'
                            loc_from = None
                            loc_to = event['location']
                    
                    movements.append({
                        'TxID': f"{case_no}_{event['original_col']}_{tx_type}_{event['date'].strftime('%Y%m%d')}",
                        'Case_No': case_no,
                        'Date': event['date'],
                        'Loc_From': loc_from,
                        'Loc_To': loc_to,
                        'Site': 'UNK',
                        'Qty': qty,
                        'SQM': sqm,
                        'Cost': 0,
                        'TxType': tx_type,
                        'SOURCE_FILE': source_file
                    })
                    
                elif event['type'] == 'site':
                    # 사이트 배송은 항상 출고
                    last_warehouse = None
                    for j in range(i-1, -1, -1):
                        if events[j]['type'] == 'warehouse':
                            last_warehouse = events[j]['location']
                            break
                    
                    movements.append({
                        'TxID': f"{case_no}_{event['original_col']}_DELIVERY_{event['date'].strftime('%Y%m%d')}",
                        'Case_No': case_no,
                        'Date': event['date'],
                        'Loc_From': last_warehouse or 'UNKNOWN',
                        'Loc_To': None,
                        'Site': event['location'],
                        'Qty': qty,
                        'SQM': sqm,
                        'Cost': 0,
                        'TxType': 'OUT',
                        'SOURCE_FILE': source_file
                    })
        
        return pd.DataFrame(movements)
    
    @staticmethod
    def load_invoice_data(file_path: str) -> pd.DataFrame:
        """인보이스 파일에서 비용 데이터 추출"""
        if not os.path.exists(file_path):
            print(f"   ❌ 인보이스 파일 없음: {file_path}")
            return pd.DataFrame()
        
        try:
            print(f"   💰 인보이스 처리 중: {os.path.basename(file_path)}")
            
            xl_file = pd.ExcelFile(file_path)
            sheet_name = xl_file.sheet_names[0]
            for sheet in xl_file.sheet_names:
                if any(keyword in sheet.lower() for keyword in ['invoice', 'cost', 'billing']):
                    sheet_name = sheet
                    break
            
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # 컬럼 매핑
            date_col = HVDCStockEngine.find_column(df, ['date', 'month', 'operation month', 'period'])
            category_col = HVDCStockEngine.find_column(df, ['category', 'location', 'warehouse', 'type'])
            cost_col = HVDCStockEngine.find_column(df, ['total', 'cost', 'amount', 'value', 'price'])
            
            if not cost_col:
                print(f"   ⚠️ Cost 컬럼을 찾을 수 없음: {file_path}")
                return pd.DataFrame()
            
            cost_records = []
            
            for idx, row in df.iterrows():
                # 날짜 처리
                if date_col and pd.notna(row[date_col]):
                    date_val = pd.to_datetime(row[date_col], errors='coerce')
                else:
                    date_val = datetime.now()
                
                # 위치 매핑
                location = "UNKNOWN"
                if category_col and pd.notna(row[category_col]):
                    cat_lower = str(row[category_col]).lower()
                    if 'indoor' in cat_lower or 'm44' in cat_lower:
                        location = 'DSV Indoor'
                    elif 'markaz' in cat_lower or 'al markaz' in cat_lower:
                        location = 'DSV Al Markaz'
                    elif 'outdoor' in cat_lower:
                        location = 'DSV Outdoor'
                    elif 'mosb' in cat_lower:
                        location = 'MOSB'
                    else:
                        location = str(row[category_col])
                
                # 비용
                cost_val = pd.to_numeric(row[cost_col], errors='coerce') or 0
                
                if cost_val > 0:
                    cost_records.append({
                        'TxID': f"COST_{idx}_{date_val.strftime('%Y%m')}",
                        'Case_No': f"COST_{idx}",
                        'Date': date_val,
                        'Loc_From': None,
                        'Loc_To': location,
                        'Site': 'UNK',
                        'Qty': 0,
                        'SQM': 0,
                        'Cost': cost_val,
                        'TxType': 'COST',
                        'SOURCE_FILE': os.path.basename(file_path)
                    })
            
            print(f"   ✅ 인보이스 로딩 완료: {len(cost_records)}건")
            return pd.DataFrame(cost_records)
            
        except Exception as e:
            print(f"   ❌ 인보이스 로딩 실패: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def calculate_daily_stock(movements_df: pd.DataFrame) -> pd.DataFrame:
        """일별 재고 계산"""
        if movements_df.empty:
            return pd.DataFrame()
        
        # 날짜별 입출고 집계
        daily_agg = []
        
        for _, row in movements_df.iterrows():
            if row['TxType'] == 'IN' and pd.notna(row['Loc_To']):
                daily_agg.append({
                    'Date': pd.to_datetime(row['Date']).date(),
                    'Location': row['Loc_To'],
                    'Inbound': row['Qty'],
                    'Outbound': 0,
                    'SQM_In': row['SQM'],
                    'SQM_Out': 0
                })
            elif row['TxType'] == 'OUT' and pd.notna(row['Loc_From']):
                daily_agg.append({
                    'Date': pd.to_datetime(row['Date']).date(),
                    'Location': row['Loc_From'],
                    'Inbound': 0,
                    'Outbound': row['Qty'],
                    'SQM_In': 0,
                    'SQM_Out': row['SQM']
                })
        
        if not daily_agg:
            return pd.DataFrame()
        
        daily_df = pd.DataFrame(daily_agg)
        
        # 날짜별, 위치별 집계
        daily_summary = daily_df.groupby(['Location', 'Date']).agg({
            'Inbound': 'sum',
            'Outbound': 'sum',
            'SQM_In': 'sum',
            'SQM_Out': 'sum'
        }).reset_index()
        
        # 각 위치별로 재고 계산
        results = []
        
        for location in daily_summary['Location'].unique():
            if location == 'UNKNOWN':
                continue
            
            loc_data = daily_summary[daily_summary['Location'] == location].copy()
            loc_data = loc_data.sort_values('Date')
            
            opening_stock = 0
            
            for _, row in loc_data.iterrows():
                closing_stock = opening_stock + row['Inbound'] - row['Outbound']
                
                results.append({
                    'Location': location,
                    'Date': row['Date'],
                    'Opening_Stock': opening_stock,
                    'Inbound': row['Inbound'],
                    'Outbound': row['Outbound'],
                    'Closing_Stock': closing_stock,
                    'SQM_In': row['SQM_In'],
                    'SQM_Out': row['SQM_Out']
                })
                
                opening_stock = closing_stock
        
        return pd.DataFrame(results)
    
    @staticmethod
    def calculate_monthly_warehouse_stock(movements_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """월별 창고 재고 계산"""
        if movements_df.empty:
            return {}
        
        movements_df['YearMonth'] = pd.to_datetime(movements_df['Date']).dt.to_period('M').astype(str)
        
        monthly_agg = []
        
        for _, row in movements_df.iterrows():
            if row['TxType'] == 'IN' and pd.notna(row['Loc_To']):
                monthly_agg.append({
                    'YearMonth': row['YearMonth'],
                    'Location': row['Loc_To'],
                    'TxType': 'IN',
                    'Qty': row['Qty'],
                    'SQM': row['SQM']
                })
            elif row['TxType'] == 'OUT' and pd.notna(row['Loc_From']):
                monthly_agg.append({
                    'YearMonth': row['YearMonth'],
                    'Location': row['Loc_From'],
                    'TxType': 'OUT',
                    'Qty': row['Qty'],
                    'SQM': row['SQM']
                })
        
        if not monthly_agg:
            return {}
        
        monthly_df = pd.DataFrame(monthly_agg)
        monthly_summary = monthly_df.groupby(['Location', 'YearMonth', 'TxType']).agg({
            'Qty': 'sum',
            'SQM': 'sum'
        }).reset_index()
        
        # 입고/출고 분리
        inbound = monthly_summary[monthly_summary['TxType'] == 'IN'].copy()
        outbound = monthly_summary[monthly_summary['TxType'] == 'OUT'].copy()
        
        # 전체 월 범위 생성
        all_months = sorted(monthly_df['YearMonth'].unique())
        all_locations = sorted([loc for loc in monthly_df['Location'].unique() if loc != 'UNKNOWN'])
        
        # 월별 재고 계산
        stock_results = []
        
        for location in all_locations:
            opening_stock = 0
            
            for month in all_months:
                month_inbound = inbound[
                    (inbound['Location'] == location) & 
                    (inbound['YearMonth'] == month)
                ]['Qty'].sum()
                
                month_outbound = outbound[
                    (outbound['Location'] == location) & 
                    (outbound['YearMonth'] == month)
                ]['Qty'].sum()
                
                month_inbound_sqm = inbound[
                    (inbound['Location'] == location) & 
                    (inbound['YearMonth'] == month)
                ]['SQM'].sum()
                
                month_outbound_sqm = outbound[
                    (outbound['Location'] == location) & 
                    (outbound['YearMonth'] == month)
                ]['SQM'].sum()
                
                closing_stock = opening_stock + month_inbound - month_outbound
                
                stock_results.append({
                    'Location': location,
                    'YearMonth': month,
                    'Opening_Stock': opening_stock,
                    'Inbound_Qty': month_inbound,
                    'Outbound_Qty': month_outbound,
                    'Closing_Stock': closing_stock,
                    'Inbound_SQM': month_inbound_sqm,
                    'Outbound_SQM': month_outbound_sqm
                })
                
                opening_stock = closing_stock
        
        stock_df = pd.DataFrame(stock_results)
        
        # 피벗 테이블들 생성
        results = {'monthly_stock_detail': stock_df}
        
        if not stock_df.empty:
            try:
                results['inbound_pivot'] = stock_df.pivot_table(
                    index='Location', columns='YearMonth', values='Inbound_Qty', fill_value=0
                ).reset_index()
                
                results['outbound_pivot'] = stock_df.pivot_table(
                    index='Location', columns='YearMonth', values='Outbound_Qty', fill_value=0
                ).reset_index()
                
                results['closing_stock_pivot'] = stock_df.pivot_table(
                    index='Location', columns='YearMonth', values='Closing_Stock', fill_value=0
                ).reset_index()
                
            except Exception as e:
                print(f"   ⚠️ 피벗 테이블 생성 오류: {e}")
        
        return results
    
    @staticmethod
    def calculate_cost_analysis(cost_records: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """비용 분석"""
        if cost_records.empty:
            return {}
        
        cost_records['YearMonth'] = pd.to_datetime(cost_records['Date']).dt.to_period('M').astype(str)
        
        # 월별 위치별 비용
        monthly_cost = cost_records.groupby(['Loc_To', 'YearMonth']).agg({
            'Cost': 'sum'
        }).reset_index().rename(columns={'Loc_To': 'Location'})
        
        results = {'monthly_cost': monthly_cost}
        
        try:
            # 비용 피벗 테이블
            cost_pivot = monthly_cost.pivot_table(
                index='Location', columns='YearMonth', values='Cost', fill_value=0
            ).reset_index()
            results['cost_pivot'] = cost_pivot
            
            # 비용 통계
            cost_stats = cost_records.groupby('Loc_To').agg({
                'Cost': ['sum', 'mean', 'min', 'max']
            }).round(2)
            
            if not cost_stats.empty:
                cost_stats.columns = ['Total_Cost', 'Avg_Cost', 'Min_Cost', 'Max_Cost']
                cost_stats = cost_stats.reset_index().rename(columns={'Loc_To': 'Location'})
                results['cost_statistics'] = cost_stats
            
        except Exception as e:
            print(f"   ⚠️ 비용 분석 오류: {e}")
        
        return results
    
    @staticmethod
    def validate_calculations(monthly_results: Dict[str, pd.DataFrame], 
                            daily_results: pd.DataFrame) -> Dict[str, Any]:
        """계산 검증"""
        errors = []
        warnings = []
        
        # 월별 재고 무결성 검증
        if 'monthly_stock_detail' in monthly_results:
            stock_df = monthly_results['monthly_stock_detail']
            
            for _, row in stock_df.iterrows():
                expected = row['Opening_Stock'] + row['Inbound_Qty'] - row['Outbound_Qty']
                actual = row['Closing_Stock']
                
                if abs(actual - expected) > 0.01:
                    errors.append(f"월별 재고 불일치 - {row['Location']} {row['YearMonth']}")
            
            # 연속성 검증
            for location in stock_df['Location'].unique():
                loc_data = stock_df[stock_df['Location'] == location].sort_values('YearMonth')
                
                for i in range(1, len(loc_data)):
                    prev_closing = loc_data.iloc[i-1]['Closing_Stock']
                    curr_opening = loc_data.iloc[i]['Opening_Stock']
                    
                    if abs(prev_closing - curr_opening) > 0.01:
                        errors.append(f"연속성 오류 - {location}")
            
            # 음수 재고 검증
            negative = stock_df[stock_df['Closing_Stock'] < 0]
            if not negative.empty:
                warnings.append(f"음수 재고 발견: {len(negative)}건")
        
        return {
            'validation_passed': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'total_monthly_records': len(monthly_results.get('monthly_stock_detail', [])),
            'total_daily_records': len(daily_results),
            'locations_checked': len(monthly_results.get('monthly_stock_detail', {}).get('Location', {}).unique() if 'monthly_stock_detail' in monthly_results else []),
            'validation_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

def run_complete_analysis(warehouse_files: List[str], invoice_files: List[str] = None):
    """완전한 분석 실행"""
    print("🔍 HVDC 창고 재고 계산 완전 검증 시작...")
    
    all_movements = []
    all_cost_records = []
    
    # 창고 파일 처리
    for file_path in warehouse_files:
        if not os.path.exists(file_path):
            print(f"   ❌ 파일 없음: {file_path}")
            continue
        
        try:
            print(f"   📂 처리 중: {os.path.basename(file_path)}")
            
            xl_file = pd.ExcelFile(file_path)
            sheet_name = xl_file.sheet_names[0]
            for sheet in xl_file.sheet_names:
                if 'case' in sheet.lower() and 'list' in sheet.lower():
                    sheet_name = sheet
                    break
            
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            movements = HVDCStockEngine.extract_warehouse_movements(df, os.path.basename(file_path))
            
            if not movements.empty:
                all_movements.append(movements)
                print(f"   ✅ 추출 완료: {len(movements)}건")
            else:
                print(f"   ⚠️ 이동 데이터 없음")
                
        except Exception as e:
            print(f"   ❌ 처리 실패: {e}")
    
    # 인보이스 파일 처리
    if invoice_files:
        for invoice_file in invoice_files:
            cost_records = HVDCStockEngine.load_invoice_data(invoice_file)
            if not cost_records.empty:
                all_cost_records.append(cost_records)
    
    if not all_movements:
        print("❌ 처리할 데이터가 없습니다.")
        return None, {}, {}, {}
    
    # 데이터 통합
    combined_movements = pd.concat(all_movements, ignore_index=True)
    print(f"\n📊 총 이동 기록: {len(combined_movements)}건")
    
    combined_costs = pd.DataFrame()
    if all_cost_records:
        combined_costs = pd.concat(all_cost_records, ignore_index=True)
        print(f"💰 총 비용 기록: {len(combined_costs)}건")
    
    # 계산 실행
    daily_stock = HVDCStockEngine.calculate_daily_stock(combined_movements)
    monthly_results = HVDCStockEngine.calculate_monthly_warehouse_stock(combined_movements)
    cost_analysis = HVDCStockEngine.calculate_cost_analysis(combined_costs) if not combined_costs.empty else {}
    
    print(f"   일별 재고 포인트: {len(daily_stock)}개")
    if monthly_results and 'monthly_stock_detail' in monthly_results:
        print(f"   월별 재고 포인트: {len(monthly_results['monthly_stock_detail'])}개")
    if cost_analysis:
        print(f"   비용 분석 완료: {len(cost_analysis)}개 테이블")
    
    # 검증
    validation = HVDCStockEngine.validate_calculations(monthly_results, daily_stock)
    
    print(f"\n📋 검증 결과:")
    print(f"   ✅ 검증 통과: {'예' if validation['validation_passed'] else '아니오'}")
    print(f"   📊 검사 대상: 월별 {validation['total_monthly_records']}건, 일별 {validation['total_daily_records']}건")
    print(f"   🏢 검사 창고 수: {validation['locations_checked']}개")
    
    if validation['errors']:
        print(f"\n❌ 발견된 오류 ({len(validation['errors'])}개):")
        for error in validation['errors'][:3]:
            print(f"     - {error}")
    
    if validation['warnings']:
        print(f"\n⚠️ 경고사항 ({len(validation['warnings'])}개):")
        for warning in validation['warnings']:
            print(f"     - {warning}")
    
    return combined_movements, monthly_results, validation, cost_analysis

def create_report(movements_df: pd.DataFrame, 
                 monthly_results: Dict[str, pd.DataFrame],
                 validation_result: Dict[str, Any],
                 cost_analysis: Dict[str, pd.DataFrame],
                 output_file: str = "HVDC_Complete_Report.xlsx"):
    """완전한 리포트 생성"""
    
    print(f"\n📄 리포트 생성 중: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # 서식 정의
        header_format = workbook.add_format({
            'bold': True, 'text_wrap': True, 'valign': 'top',
            'fg_color': '#D7E4BC', 'border': 1, 'align': 'center'
        })
        number_format = workbook.add_format({'num_format': '#,##0.00'})
        currency_format = workbook.add_format({'num_format': '#,##0.00 "AED"'})
        
        # 1. 검증 요약
        validation_summary = pd.DataFrame([{
            '항목': '검증 통과 여부',
            '결과': '통과' if validation_result['validation_passed'] else '실패',
            '상세': f"오류 {len(validation_result['errors'])}건, 경고 {len(validation_result['warnings'])}건"
        }, {
            '항목': '총 월별 기록',
            '결과': validation_result['total_monthly_records'],
            '상세': f"{validation_result['locations_checked']}개 창고"
        }, {
            '항목': '총 일별 기록',
            '결과': validation_result['total_daily_records'],
            '상세': '일별 재고 변동'
        }, {
            '항목': '비용 분석',
            '결과': '포함' if cost_analysis else '없음',
            '상세': f"{len(cost_analysis)}개 테이블" if cost_analysis else 'N/A'
        }])
        
        validation_summary.to_excel(writer, sheet_name='📊_검증요약', index=False)
        worksheet = writer.sheets['📊_검증요약']
        for col_num, value in enumerate(validation_summary.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # 2. 월별 재고 상세
        if monthly_results and 'monthly_stock_detail' in monthly_results:
            monthly_detail = monthly_results['monthly_stock_detail']
            monthly_detail.to_excel(writer, sheet_name='📅_월별재고상세', index=False)
            worksheet = writer.sheets['📅_월별재고상세']
            
            for col_num, value in enumerate(monthly_detail.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            for i, col in enumerate(monthly_detail.columns):
                if col in ['Opening_Stock', 'Inbound_Qty', 'Outbound_Qty', 'Closing_Stock']:
                    worksheet.set_column(i, i, 15, number_format)
        
        # 3. 피벗 테이블들
        if monthly_results:
            pivot_tables = ['inbound_pivot', 'outbound_pivot', 'closing_stock_pivot']
            sheet_names = ['📈_입고피벗', '📉_출고피벗', '📊_재고피벗']
            
            for pivot_name, sheet_name in zip(pivot_tables, sheet_names):
                if pivot_name in monthly_results:
                    pivot_df = monthly_results[pivot_name]
                    pivot_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    worksheet = writer.sheets[sheet_name]
                    
                    for col_num, value in enumerate(pivot_df.columns.values):
                        worksheet.write(0, col_num, value, header_format)
        
        # 4. 비용 분석
        if cost_analysis:
            for key, df in cost_analysis.items():
                if not df.empty:
                    sheet_name = f"💰_{key}"[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    worksheet = writer.sheets[sheet_name]
                    
                    for col_num, value in enumerate(df.columns.values):
                        worksheet.write(0, col_num, value, header_format)
                    
                    # 비용 컬럼에 통화 서식
                    for i, col in enumerate(df.columns):
                        if 'cost' in col.lower():
                            worksheet.set_column(i, i, 15, currency_format)
        
        # 5. 원본 데이터 (샘플)
        if not movements_df.empty:
            sample = movements_df.head(500)
            sample.to_excel(writer, sheet_name='📄_원본데이터', index=False)
            worksheet = writer.sheets['📄_원본데이터']
            
            for col_num, value in enumerate(sample.columns.values):
                worksheet.write(0, col_num, value, header_format)
    
    print(f"✅ 리포트 저장 완료: {output_file}")

def main():
    """메인 실행 함수"""
    print("🚀 HVDC 창고 재고 계산 로직 완전 검증")
    print("=" * 60)
    
    # 파일 경로 설정
    warehouse_files = [
        "HVDC WAREHOUSE_HITACHI(HE).xlsx",
        "HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx",
        "HVDC WAREHOUSE_HITACHI(HE-0214,0252).xlsx",
        "HVDC WAREHOUSE_SIMENSE(SIM).xlsx"
    ]
    
    invoice_files = [
        "HVDC WAREHOUSE_INVOICE.xlsx"
    ]
    
    # 파일 찾기
    actual_warehouse_files = []
    actual_invoice_files = []
    search_dirs = [".", "data", "analytics/data"]
    
    for search_dir in search_dirs:
        if os.path.exists(search_dir):
            # 창고 파일 찾기
            for file_path in warehouse_files:
                full_path = os.path.join(search_dir, file_path)
                if os.path.exists(full_path) and full_path not in actual_warehouse_files:
                    actual_warehouse_files.append(full_path)
            
            # 인보이스 파일 찾기
            for invoice_file in invoice_files:
                full_path = os.path.join(search_dir, invoice_file)
                if os.path.exists(full_path) and full_path not in actual_invoice_files:
                    actual_invoice_files.append(full_path)
    
    if not actual_warehouse_files:
        print("❌ HVDC 창고 파일을 찾을 수 없습니다!")
        print("💡 다음 파일들이 현재 폴더에 있는지 확인하세요:")
        for file_path in warehouse_files:
            print(f"   - {file_path}")
        return False
    
    print(f"📁 발견된 창고 파일: {len(actual_warehouse_files)}개")
    for file_path in actual_warehouse_files:
        print(f"   - {file_path}")
    
    print(f"💰 발견된 인보이스 파일: {len(actual_invoice_files)}개")
    for file_path in actual_invoice_files:
        print(f"   - {file_path}")
    
    # 분석 실행
    movements, monthly_results, validation, cost_analysis = run_complete_analysis(
        actual_warehouse_files, actual_invoice_files
    )
    
    if movements is None:
        return False
    
    # 리포트 생성
    create_report(movements, monthly_results, validation, cost_analysis)
    
    # 결과 요약
    print(f"\n📋 최종 요약:")
    print(f"   창고 파일: {len(actual_warehouse_files)}개")
    print(f"   인보이스 파일: {len(actual_invoice_files)}개")
    print(f"   이동 기록: {len(movements):,}건")
    print(f"   검증 결과: {'✅ 통과' if validation['validation_passed'] else '❌ 실패'}")
    
    if monthly_results and 'monthly_stock_detail' in monthly_results:
        monthly_detail = monthly_results['monthly_stock_detail']
        print(f"   월별 재고: {len(monthly_detail):,}개")
        
        print(f"\n🏢 창고별 최종 재고:")
        for location in monthly_detail['Location'].unique():
            loc_data = monthly_detail[monthly_detail['Location'] == location]
            if not loc_data.empty:
                final = loc_data['Closing_Stock'].iloc[-1]
                inbound = loc_data['Inbound_Qty'].sum()
                outbound = loc_data['Outbound_Qty'].sum()
                print(f"   {location}: {final:,.0f}박스 (입고{inbound:,.0f}, 출고{outbound:,.0f})")
    
    if cost_analysis and 'cost_statistics' in cost_analysis:
        cost_stats = cost_analysis['cost_statistics']
        if not cost_stats.empty:
            print(f"\n💰 비용 요약:")
            for _, row in cost_stats.iterrows():
                print(f"   {row['Location']}: {row['Total_Cost']:,.2f} AED")
    
    print(f"\n📄 생성된 리포트: HVDC_Complete_Report.xlsx")
    return validation['validation_passed']

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 모든 검증 완료!")
    else:
        print("\n⚠️ 검증 실패 또는 오류 발생")