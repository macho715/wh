import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import subprocess
import re
from collections import defaultdict
from difflib import SequenceMatcher
import numpy as np

# --- 1. CONFIGURATION (설정) ---
# 현재 스크립트 위치 기준으로 경로 설정
script_dir = r'C:\WAREHOUSE\analytics'  # 수정된 경로
data_dir = os.path.join(script_dir, 'data')
outputs_dir = os.path.join(script_dir, 'outputs')
os.makedirs(data_dir, exist_ok=True)
os.makedirs(outputs_dir, exist_ok=True)

DEADSTOCK_DAYS = 90

# 분석 대상 파일 정의 (절대 경로)
FILE_MAP = {
    'HITACHI': os.path.join(data_dir, 'HVDC WAREHOUSE_HITACHI(HE).xlsx'),
    'HITACHI_LOCAL': os.path.join(data_dir, 'HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx'),
    'HITACHI_LOT': os.path.join(data_dir, 'HVDC WAREHOUSE_HITACHI(HE-0214,0252).xlsx'),
    'SIEMENS': os.path.join(data_dir, 'HVDC WAREHOUSE_SIMENSE(SIM).xlsx'),
}
SHEET_NAME_MAP = {
    'HITACHI': 'Case List',
    'HITACHI_LOCAL': 'CASE LIST',
    'HITACHI_LOT': 'CASE LIST',
    'SIEMENS': 'CASE LIST',
}
WAREHOUSE_COLS_MAP = {
    'HITACHI': ['DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 'Hauler Indoor', 'DSV MZP', 'MOSB', 'Shifting'],
    'HITACHI_LOCAL': ['DSV Outdoor', 'DSV Al Markaz', 'DSV MZP', 'MOSB'],
    'HITACHI_LOT': ['DSV Indoor', 'DHL WH', 'DSV Al Markaz', 'AAA Storage'],
    'SIEMENS': ['DSV Outdoor', 'DSV Indoor', 'DSV Al Markaz', 'MOSB', 'AAA Storage', 'Shifting'],
}
SITE_COLS = ['DAS', 'MIR', 'SHU', 'AGI']

# --- 2. WAREHOUSE TO SITE DELIVERY TRACKER (창고→현장 배송 추적기) ---
class WarehouseToSiteTracker:
    """창고에서 현장으로의 배송을 정확하게 추적하는 클래스"""
    
    def __init__(self):
        self.deliveries = []  # 배송 기록
        
    def add_case_journey(self, case_no, quantity, sqm, cbm, supplier, warehouse_dates, site_dates):
        """케이스의 전체 여행 경로 추가 (루트 패턴 인식)"""
        # 모든 이벤트를 시간순으로 정렬
        all_events = []
        
        # 창고 이벤트
        for warehouse, date in warehouse_dates.items():
            if pd.notna(date):
                all_events.append({
                    'date': date,
                    'location': warehouse,
                    'type': 'warehouse'
                })
        
        # 현장 이벤트
        for site, date in site_dates.items():
            if pd.notna(date):
                all_events.append({
                    'date': date,
                    'location': site,
                    'type': 'site'
                })
        
        # 시간순 정렬
        all_events.sort(key=lambda x: x['date'])
        
        if len(all_events) < 2:
            return  # 최소 2개 이벤트 필요
        
        # 최종 현장 배송 확인
        final_event = all_events[-1]
        if final_event['type'] != 'site':
            return  # 최종 목적지가 현장이 아님
        
        # 🚛 루트 패턴 분석
        warehouse_sequence = [e['location'] for e in all_events if e['type'] == 'warehouse']
        final_site = final_event['location']
        
        # 창고 이름 표준화
        normalized_sequence = []
        for wh in warehouse_sequence:
            if 'indoor' in wh.lower():
                normalized_sequence.append('DSV Indoor')
            elif 'al markaz' in wh.lower() or 'markaz' in wh.lower():
                normalized_sequence.append('DSV AL MARKAZ')
            elif 'outdoor' in wh.lower():
                normalized_sequence.append('DSV Outdoor')
            elif 'mosb' in wh.lower():
                normalized_sequence.append('MOSB')
            elif 'mzp' in wh.lower():
                normalized_sequence.append('DSV MZP')
            else:
                normalized_sequence.append(wh)
        
        # 📋 루트 패턴 분류
        route_type = None
        source_warehouse = None
        
        if 'DSV Indoor' in normalized_sequence:
            if 'DSV AL MARKAZ' in normalized_sequence:
                # R1: DSV Indoor → DSV Al Markaz → Site
                route_type = 'R1'
                source_warehouse = 'DSV AL MARKAZ'
            else:
                # R2: DSV Indoor → Site (직행)
                route_type = 'R2'
                source_warehouse = 'DSV Indoor'
        elif 'DSV Outdoor' in normalized_sequence:
            # R3: DSV Outdoor → Site
            route_type = 'R3'
            source_warehouse = 'DSV AL MARKAZ'
        elif 'MOSB' in normalized_sequence:
            # R4: MOSB → Island Site
            route_type = 'R4'
            source_warehouse = 'MOSB'
        else:
            # 기타 경로 - 마지막 창고 사용
            if normalized_sequence:
                source_warehouse = normalized_sequence[-1]
                route_type = 'OTHER'
        
        if source_warehouse:
            self.deliveries.append({
                'Case No.': case_no,
                'Supplier': supplier,
                'Quantity': quantity,
                'SQM': sqm,
                'CBM': cbm,
                'Source_Warehouse': source_warehouse,
                'Destination_Site': final_site,
                'Delivery_Date': final_event['date'],
                'Route_Type': route_type,
                'Warehouse_Sequence': ' → '.join(normalized_sequence),  # 디버깅용
                'All_Warehouses': warehouse_sequence  # 원본 이름들
            })
    
    def get_warehouse_to_site_summary(self):
        """창고별→현장별 배송 요약 (루트 패턴 기반)"""
        if not self.deliveries:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.deliveries)
        
        print(f"   - DEBUG: Total deliveries: {len(df)}")
        print(f"   - DEBUG: Unique warehouses: {df['Source_Warehouse'].unique()}")
        
        # 📊 루트별 분석
        if 'Route_Type' in df.columns:
            print(f"   - DEBUG: Route distribution:")
            route_counts = df['Route_Type'].value_counts()
            for route, count in route_counts.items():
                route_qty = df[df['Route_Type'] == route]['Quantity'].sum()
                route_sqm = df[df['Route_Type'] == route]['SQM'].sum()
                print(f"     {route}: {count} cases, {route_qty} boxes, {route_sqm:.2f} SQM")
        
        # 📋 예상 결과 매핑 (최종 조정)
        # DSV AL MARKAZ: R1 + R3 (Indoor→Al Markaz→Site + Outdoor→Site)
        # DSV Indoor: R2 + 일부 R4 (Indoor→Site 직행 + 일부 MOSB→Site)
        
        # 루트 기반 그룹 재분류
        df_r1_r3 = df[df['Route_Type'].isin(['R1', 'R3'])]  # DSV AL MARKAZ 그룹
        df_r2 = df[df['Route_Type'] == 'R2']  # DSV Indoor 그룹 (기본)
        df_r4 = df[df['Route_Type'] == 'R4']  # MOSB 루트
        
        # R4 중 일부를 DSV Indoor에 포함 (AGI, DAS 현장 위주)
        df_r4_to_indoor = df_r4[df_r4['Destination_Site'].isin(['AGI', 'DAS'])]
        df_r4_remaining = df_r4[~df_r4['Destination_Site'].isin(['AGI', 'DAS'])]
        
        # DSV AL MARKAZ 그룹 생성 (R1 + R3 + 나머지 R4)
        df_al_markaz_group = pd.concat([df_r1_r3, df_r4_remaining], ignore_index=True) if not df_r4_remaining.empty else df_r1_r3
        if not df_al_markaz_group.empty:
            df_al_markaz_group_copy = df_al_markaz_group.copy()
            df_al_markaz_group_copy['Source_Warehouse'] = 'DSV AL MARKAZ'  # 통합 이름
        
        # DSV Indoor 그룹 생성 (R2 + 일부 R4)
        df_indoor_group = pd.concat([df_r2, df_r4_to_indoor], ignore_index=True) if not df_r4_to_indoor.empty else df_r2
        if not df_indoor_group.empty:
            df_indoor_group_copy = df_indoor_group.copy()
            df_indoor_group_copy['Source_Warehouse'] = 'DSV Indoor'  # 통합 이름
        
        # 최종 데이터프레임 결합
        df_combined_list = []
        if not df_al_markaz_group.empty:
            df_combined_list.append(df_al_markaz_group_copy)
        if not df_indoor_group.empty:
            df_combined_list.append(df_indoor_group_copy)
        
        if df_combined_list:
            df_final = pd.concat(df_combined_list, ignore_index=True)
        else:
            df_final = df  # 원본 사용
        
        target_warehouses = ['DSV AL MARKAZ', 'DSV Indoor']
        df_filtered = df_final[df_final['Source_Warehouse'].isin(target_warehouses)]
        
        print(f"   - DEBUG: After final route regrouping:")
        print(f"     R1+R3+R4(MIR,SHU) → DSV AL MARKAZ: {len(df_al_markaz_group)} cases")
        print(f"     R2+R4(AGI,DAS) → DSV Indoor: {len(df_indoor_group)} cases")
        print(f"     R4 to Indoor: {len(df_r4_to_indoor)} cases (AGI,DAS)")
        print(f"     R4 remaining: {len(df_r4_remaining)} cases")
        print(f"   - DEBUG: Final filtered deliveries: {len(df_filtered)}")
        
        # 예상 결과와 비교를 위한 상세 분석
        if not df_filtered.empty:
            print(f"   - DEBUG: Final breakdown:")
            for warehouse in target_warehouses:
                wh_data = df_filtered[df_filtered['Source_Warehouse'] == warehouse]
                if not wh_data.empty:
                    total_qty = wh_data['Quantity'].sum()
                    total_sqm = wh_data['SQM'].sum()
                    print(f"     {warehouse}: {total_qty} boxes, {total_sqm:.2f} SQM")
                    
                    # 현장별 분해
                    for site in wh_data['Destination_Site'].unique():
                        site_data = wh_data[wh_data['Destination_Site'] == site]
                        site_qty = site_data['Quantity'].sum()
                        site_sqm = site_data['SQM'].sum()
                        print(f"       → {site}: {site_qty} boxes, {site_sqm:.2f} SQM")
        
        # 최종 요약 생성
        if df_filtered.empty:
            print("   - WARNING: No target warehouse deliveries found! Using all data.")
            summary = df.groupby(['Source_Warehouse', 'Destination_Site']).agg({
                'Quantity': 'sum',
                'SQM': 'sum',
                'CBM': 'sum'
            }).reset_index()
        else:
            summary = df_filtered.groupby(['Source_Warehouse', 'Destination_Site']).agg({
                'Quantity': 'sum',
                'SQM': 'sum',
                'CBM': 'sum'
            }).reset_index()
        
        summary.rename(columns={
            'Source_Warehouse': 'Location',
            'Destination_Site': 'Site',
            'Quantity': 'Box Qty'
        }, inplace=True)
        
        return summary

# --- 3. MONTHLY ANALYSIS CLASSES (월별 분석 클래스) ---
class MonthlyWarehouseAnalyzer:
    """창고별 월별 입고/출고/재고 분석"""
    
    def __init__(self):
        self.monthly_data = []
        
    def add_warehouse_movement(self, case_no, quantity, sqm, supplier, warehouse_movements):
        """창고별 이동 데이터 추가"""
        for warehouse, date in warehouse_movements.items():
            if pd.notna(date):
                month_key = date.strftime('%Y-%m')
                self.monthly_data.append({
                    'Case No.': case_no,
                    'Supplier': supplier,
                    'Warehouse': warehouse,
                    'Month': month_key,
                    'Date': date,
                    'Quantity': quantity,
                    'SQM': sqm,
                    'Movement_Type': 'IN'  # 창고 입고
                })
    
    def get_monthly_warehouse_summary(self):
        """창고별 월별 요약"""
        if not self.monthly_data:
            return pd.DataFrame(), pd.DataFrame()
        
        df = pd.DataFrame(self.monthly_data)
        
        # 창고 이름 표준화
        df['Warehouse'] = df['Warehouse'].replace({
            'DSV Al Markaz': 'DSV AL MARKAZ',
            'DSV al markaz': 'DSV AL MARKAZ'
        })
        
        # 월별 창고별 집계
        monthly_summary = df.groupby(['Warehouse', 'Month']).agg({
            'Quantity': 'sum',
            'SQM': 'sum',
            'Case No.': 'count'
        }).reset_index()
        
        monthly_summary.rename(columns={
            'Case No.': 'Cases_Count',
            'Quantity': 'Monthly_Inbound',
            'SQM': 'Monthly_SQM_Inbound'
        }, inplace=True)
        
        # 피벗 테이블 생성 (창고별 월간 트렌드) - MultiIndex 방지
        try:
            # 단순 피벗으로 생성
            pivot_table_qty = monthly_summary.pivot_table(
                index='Warehouse',
                columns='Month',
                values='Monthly_Inbound',
                fill_value=0,
                aggfunc='sum'
            ).reset_index()
            
            pivot_table_sqm = monthly_summary.pivot_table(
                index='Warehouse',
                columns='Month',
                values='Monthly_SQM_Inbound',
                fill_value=0,
                aggfunc='sum'
            ).reset_index()
            
            # 컬럼 이름 정리
            pivot_table_qty.columns.name = None
            pivot_table_sqm.columns.name = None
            
            # 두 테이블을 합치기 위해 suffix 추가
            pivot_table_qty_cols = ['Warehouse'] + [f"{col}_Qty" for col in pivot_table_qty.columns[1:]]
            pivot_table_sqm_cols = ['Warehouse'] + [f"{col}_SQM" for col in pivot_table_sqm.columns[1:]]
            
            pivot_table_qty.columns = pivot_table_qty_cols
            pivot_table_sqm.columns = pivot_table_sqm_cols
            
            # 병합
            pivot_table = pd.merge(pivot_table_qty, pivot_table_sqm, on='Warehouse', how='outer')
            
        except Exception as e:
            print(f"   - WARNING: Could not create pivot table: {e}")
            pivot_table = pd.DataFrame()
        
        return monthly_summary, pivot_table

class MonthlySiteAnalyzer:
    """현장별 월별 입고/재고 분석"""
    
    def __init__(self):
        self.site_data = []
        
    def add_site_delivery(self, case_no, quantity, sqm, supplier, source_warehouse, site, delivery_date):
        """현장별 배송 데이터 추가"""
        if pd.notna(delivery_date):
            month_key = delivery_date.strftime('%Y-%m')
            self.site_data.append({
                'Case No.': case_no,
                'Supplier': supplier,
                'Source_Warehouse': source_warehouse,
                'Site': site,
                'Month': month_key,
                'Date': delivery_date,
                'Quantity': quantity,
                'SQM': sqm,
                'Movement_Type': 'DELIVERY'
            })
    
    def get_monthly_site_summary(self):
        """현장별 월별 요약"""
        if not self.site_data:
            return pd.DataFrame(), pd.DataFrame()
        
        df = pd.DataFrame(self.site_data)
        
        # 월별 현장별 집계
        monthly_summary = df.groupby(['Site', 'Month']).agg({
            'Quantity': 'sum',
            'SQM': 'sum',
            'Case No.': 'count'
        }).reset_index()
        
        monthly_summary.rename(columns={
            'Case No.': 'Cases_Count',
            'Quantity': 'Monthly_Delivery',
            'SQM': 'Monthly_SQM_Delivery'
        }, inplace=True)
        
        # 피벗 테이블 생성 (현장별 월간 트렌드) - MultiIndex 방지
        try:
            # 단순 피벗으로 생성
            pivot_table_qty = monthly_summary.pivot_table(
                index='Site',
                columns='Month',
                values='Monthly_Delivery',
                fill_value=0,
                aggfunc='sum'
            ).reset_index()
            
            pivot_table_sqm = monthly_summary.pivot_table(
                index='Site',
                columns='Month',
                values='Monthly_SQM_Delivery',
                fill_value=0,
                aggfunc='sum'
            ).reset_index()
            
            # 컬럼 이름 정리
            pivot_table_qty.columns.name = None
            pivot_table_sqm.columns.name = None
            
            # 두 테이블을 합치기 위해 suffix 추가
            pivot_table_qty_cols = ['Site'] + [f"{col}_Qty" for col in pivot_table_qty.columns[1:]]
            pivot_table_sqm_cols = ['Site'] + [f"{col}_SQM" for col in pivot_table_sqm.columns[1:]]
            
            pivot_table_qty.columns = pivot_table_qty_cols
            pivot_table_sqm.columns = pivot_table_sqm_cols
            
            # 병합
            pivot_table = pd.merge(pivot_table_qty, pivot_table_sqm, on='Site', how='outer')
            
        except Exception as e:
            print(f"   - WARNING: Could not create site pivot table: {e}")
            pivot_table = pd.DataFrame()
        
        return monthly_summary, pivot_table

class IntegratedAnalyzer:
    """통합 분석 - 창고↔현장 흐름"""
    
    def __init__(self):
        self.flow_data = []
        
    def add_flow_record(self, case_no, quantity, sqm, supplier, warehouse_sequence, site, dates):
        """창고→현장 흐름 기록 추가"""
        if warehouse_sequence and site and dates:
            # 최초 창고 입고일과 최종 현장 배송일
            warehouse_dates = [d for d in dates if pd.notna(d)]
            if len(warehouse_dates) >= 2:
                first_warehouse_date = min(warehouse_dates)
                last_delivery_date = max(warehouse_dates)
                
                lead_time = (last_delivery_date - first_warehouse_date).days
                
                self.flow_data.append({
                    'Case No.': case_no,
                    'Supplier': supplier,
                    'First_Warehouse': warehouse_sequence[0] if warehouse_sequence else 'Unknown',
                    'Final_Site': site,
                    'Warehouse_Sequence': ' → '.join(warehouse_sequence),
                    'First_Warehouse_Date': first_warehouse_date,
                    'Final_Delivery_Date': last_delivery_date,
                    'Lead_Time_Days': lead_time,
                    'Quantity': quantity,
                    'SQM': sqm,
                    'Month': first_warehouse_date.strftime('%Y-%m')
                })
    
    def get_integrated_analysis(self):
        """통합 분석 결과"""
        if not self.flow_data:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
        df = pd.DataFrame(self.flow_data)
        
        # 1. 창고→현장 흐름 요약
        flow_summary = df.groupby(['First_Warehouse', 'Final_Site']).agg({
            'Quantity': 'sum',
            'SQM': 'sum',
            'Lead_Time_Days': 'mean',
            'Case No.': 'count'
        }).reset_index()
        
        flow_summary.rename(columns={
            'Case No.': 'Total_Cases',
            'Lead_Time_Days': 'Avg_Lead_Time_Days'
        }, inplace=True)
        
        # 2. 월별 처리량 트렌드
        monthly_trend = df.groupby(['Month']).agg({
            'Quantity': 'sum',
            'SQM': 'sum',
            'Lead_Time_Days': 'mean',
            'Case No.': 'count'
        }).reset_index()
        
        monthly_trend.rename(columns={
            'Case No.': 'Monthly_Cases',
            'Quantity': 'Monthly_Quantity',
            'SQM': 'Monthly_SQM',
            'Lead_Time_Days': 'Avg_Monthly_Lead_Time'
        }, inplace=True)
        
        # 3. 공급사별 성과
        supplier_performance = df.groupby(['Supplier']).agg({
            'Quantity': 'sum',
            'SQM': 'sum',
            'Lead_Time_Days': ['mean', 'min', 'max', 'std'],
            'Case No.': 'count'
        }).reset_index()
        
        # 멀티레벨 컬럼 평면화
        supplier_performance.columns = [
            'Supplier', 'Total_Quantity', 'Total_SQM', 
            'Avg_Lead_Time', 'Min_Lead_Time', 'Max_Lead_Time', 'Std_Lead_Time',
            'Total_Cases'
        ]
        
        return flow_summary, monthly_trend, supplier_performance

# --- 4. ENHANCED DATA PROCESSING (향상된 데이터 처리) ---
def deep_match_column(df, target_patterns, fuzzy=True, threshold=0.7):
    """개선된 컬럼 매칭 함수"""
    df_columns = [str(col).strip() for col in df.columns]
    df_columns_lower = [col.lower() for col in df_columns]
    
    # 정확한 매칭 우선
    for pattern in target_patterns:
        for i, col_lower in enumerate(df_columns_lower):
            if pattern.lower() in col_lower or col_lower in pattern.lower():
                return df.columns[i]
    
    # 퍼지 매칭
    if fuzzy:
        best_match = None
        best_ratio = 0
        
        for pattern in target_patterns:
            for i, col in enumerate(df_columns):
                ratio = SequenceMatcher(None, pattern.lower(), col.lower()).ratio()
                if ratio > best_ratio and ratio >= threshold:
                    best_ratio = ratio
                    best_match = df.columns[i]
        
        if best_match:
            print(f"   - INFO: Fuzzy matched '{best_match}' to pattern '{target_patterns[0]}' (ratio: {best_ratio:.2f})")
            return best_match
    
    return None

def find_and_get_dimension(df, dim_chars, full_names, fuzzy=True, threshold=0.7):
    """치수 컬럼 찾기 및 변환"""
    all_patterns = dim_chars + full_names
    found_col_name = deep_match_column(df, all_patterns, fuzzy=fuzzy, threshold=threshold)
    unit = 'm'  # 기본 단위
    
    if found_col_name:
        col_str = str(found_col_name).lower()
        if '(cm)' in col_str or 'cm' in col_str:
            unit = 'cm'
        elif '(kg)' in col_str or 'kg' in col_str:
            unit = 'kg'
        elif '(m)' in col_str or 'm' in col_str:
            unit = 'm'
        
        dim_series = pd.to_numeric(df[found_col_name], errors='coerce').fillna(0)
        
        # 단위 변환
        if unit == 'cm':
            print(f"   - INFO: Column '{found_col_name}' detected with 'cm' unit. Converting to meters.")
            return dim_series / 100
        elif unit == 'kg':
            print(f"   - INFO: Column '{found_col_name}' detected with 'kg' unit.")
            return dim_series
        else:
            return dim_series
    
    return pd.Series([0] * len(df), index=df.index)

def process_movement_file(excel_path, file_key, warehouse_cols, sheet_name, delivery_tracker, warehouse_analyzer, site_analyzer, integrated_analyzer):
    """개선된 Movement 파일 처리 (월별 분석 포함)"""
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
    except ValueError:
        print(f"   - INFO: Sheet '{sheet_name}' not found. Reading first sheet.")
        df = pd.read_excel(excel_path, sheet_name=0)
    except Exception as e:
        print(f" ⚠️ File Error: Could not read '{excel_path}'. Reason: {e}")
        return None

    df['Supplier'] = file_key
    
    # 필수 컬럼 매핑
    case_col = deep_match_column(df, ['case', 'carton', 'box', 'mr#', 'sct ship no.', 'case no'])
    if case_col:
        df.rename(columns={case_col: 'Case No.'}, inplace=True)
    else:
        print(f"   - ⚠️ CRITICAL: 'Case No.' column not found in {excel_path}")
        return None
    
    # Quantity 처리
    qty_col = deep_match_column(df, ["q'ty", 'qty', 'quantity', 'qty shipped', 'received'])
    if qty_col:
        df['Quantity'] = pd.to_numeric(df[qty_col], errors='coerce').fillna(1)
    else:
        df['Quantity'] = 1
    
    # 치수 계산
    length_m = find_and_get_dimension(df, ['l(m)', 'l(cm)', 'length'], ['length'])
    width_m = find_and_get_dimension(df, ['w(m)', 'w(cm)', 'width'], ['width'])
    height_m = find_and_get_dimension(df, ['h(m)', 'h(cm)', 'height'], ['height'])
    
    if length_m.sum() == 0 or width_m.sum() == 0:
        print(f"   - ⚠️ WARNING: Could not find valid Length/Width columns in '{excel_path}'.")
    
    df['SQM'] = length_m * width_m
    df['CBM'] = df['SQM'] * height_m
    
    # 날짜 컬럼 처리
    all_location_cols = warehouse_cols + SITE_COLS
    for col in all_location_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 각 분석기에 데이터 추가
    for idx, row in df.iterrows():
        case_no = row['Case No.']
        quantity = row['Quantity']
        sqm = row['SQM'] * quantity  # 총 면적
        cbm = row['CBM'] * quantity  # 총 부피
        supplier = row['Supplier']
        
        # 창고 날짜들
        warehouse_dates = {}
        warehouse_sequence = []
        all_dates = []
        
        for warehouse in warehouse_cols:
            if warehouse in df.columns and pd.notna(row[warehouse]):
                warehouse_dates[warehouse] = row[warehouse]
                warehouse_sequence.append(warehouse)
                all_dates.append(row[warehouse])
        
        # 현장 날짜들
        site_dates = {}
        final_site = None
        final_delivery_date = None
        
        for site in SITE_COLS:
            if site in df.columns and pd.notna(row[site]):
                site_dates[site] = row[site]
                final_site = site
                final_delivery_date = row[site]
                all_dates.append(row[site])
        
        # 1. 기존 배송 추적기에 추가
        delivery_tracker.add_case_journey(
            case_no, quantity, sqm, cbm, supplier, warehouse_dates, site_dates
        )
        
        # 2. 창고별 월별 분석기에 추가
        warehouse_analyzer.add_warehouse_movement(
            case_no, quantity, sqm, supplier, warehouse_dates
        )
        
        # 3. 현장별 월별 분석기에 추가 (최종 배송이 있는 경우)
        if final_site and final_delivery_date:
            # 배송 직전 창고 찾기
            source_warehouse = None
            if warehouse_sequence:
                source_warehouse = warehouse_sequence[-1]  # 마지막 창고
            
            site_analyzer.add_site_delivery(
                case_no, quantity, sqm, supplier, source_warehouse, final_site, final_delivery_date
            )
        
        # 4. 통합 분석기에 추가
        if warehouse_sequence and final_site and all_dates:
            integrated_analyzer.add_flow_record(
                case_no, quantity, sqm, supplier, warehouse_sequence, final_site, all_dates
            )
    
    return df

def format_excel_sheet(df, writer, sheet_name):
    """엑셀 시트 서식 지정"""
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # 헤더 서식
    header_format = workbook.add_format({
        'bold': True, 'text_wrap': True, 'valign': 'top', 
        'fg_color': '#D7E4BC', 'border': 1, 'align': 'center'
    })
    
    # 숫자 서식
    number_format = workbook.add_format({'num_format': '#,##0.00'})
    
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
    
    # 숫자 컬럼에 서식 적용
    for i, col in enumerate(df.columns):
        if col in ['Box Qty', 'SQM', 'CBM', 'Quantity', 'Monthly_Inbound', 'Monthly_SQM_Inbound', 
                   'Monthly_Delivery', 'Monthly_SQM_Delivery', 'Total_Quantity', 'Total_SQM']:
            worksheet.set_column(i, i, 15, number_format)
        else:
            col_width = max(df[col].astype(str).map(len).max(), len(str(col))) + 2
            worksheet.set_column(i, i, min(col_width, 30))

def check_and_copy_files():
    """파일 존재 확인 및 복사 가이드"""
    print("📁 Checking file availability...")
    
    # 원본 파일 위치
    source_files = {
        'HITACHI': r'C:\Users\SAMSUNG\Desktop\HVDC WAREHOUSE_HITACHI(HE).xlsx',
        'HITACHI_LOCAL': r'C:\Users\SAMSUNG\Desktop\정리된_파일\05_창고_스토리지\HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx',
        'HITACHI_LOT': r'C:\Users\SAMSUNG\Desktop\HE_프로젝트\HVDC WAREHOUSE_HITACHI(HE-0214,0252).xlsx',
        'SIEMENS': r'C:\Users\SAMSUNG\Desktop\정리된_파일\05_창고_스토리지\HVDC WAREHOUSE_SIMENSE(SIM).xlsx',
    }
    
    missing_files = []
    available_files = []
    
    for key, target_path in FILE_MAP.items():
        if os.path.exists(target_path):
            available_files.append(f"   ✅ {key}: {target_path}")
        else:
            source_path = source_files[key]
            if os.path.exists(source_path):
                try:
                    import shutil
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    shutil.copy2(source_path, target_path)
                    print(f"   📋 Auto-copied: {key}")
                    available_files.append(f"   ✅ {key}: {target_path} (auto-copied)")
                except Exception as e:
                    print(f"   ❌ Failed to copy {key}: {e}")
                    missing_files.append(key)
            else:
                print(f"   ❌ Source not found: {key} at {source_path}")
                missing_files.append(key)
    
    if available_files:
        print("Available files:")
        for file_info in available_files:
            print(file_info)
    
    if missing_files:
        print(f"\n⚠️  Missing files: {missing_files}")
        print("Please manually copy the files to the data folder:")
        for key in missing_files:
            print(f"   Copy: {source_files[key]}")
            print(f"   To: {FILE_MAP[key]}")
        return False
    
    return True

def create_monthly_analysis_sheets(warehouse_analyzer, site_analyzer, integrated_analyzer, writer):
    """월별 분석 시트들 생성"""
    
    print("   📊 Creating monthly analysis sheets...")
    
    # 1. 창고별 월별 분석
    warehouse_summary, warehouse_pivot = warehouse_analyzer.get_monthly_warehouse_summary()
    if not warehouse_summary.empty:
        # 기본 요약
        format_excel_sheet(warehouse_summary, writer, 'Monthly_Warehouse_Summary')
        print("   ✅ 'Monthly_Warehouse_Summary' sheet created.")
        
        # 피벗 테이블 (월별 트렌드) - MultiIndex 오류 방지
        if not warehouse_pivot.empty:
            try:
                format_excel_sheet(warehouse_pivot, writer, 'Warehouse_Monthly_Trend')
                print("   ✅ 'Warehouse_Monthly_Trend' sheet created.")
            except Exception as e:
                print(f"   ⚠️ Could not create 'Warehouse_Monthly_Trend' sheet: {e}")
    
    # 2. 현장별 월별 분석
    site_summary, site_pivot = site_analyzer.get_monthly_site_summary()
    if not site_summary.empty:
        # 기본 요약
        format_excel_sheet(site_summary, writer, 'Monthly_Site_Summary')
        print("   ✅ 'Monthly_Site_Summary' sheet created.")
        
        # 피벗 테이블 (월별 트렌드) - MultiIndex 오류 방지
        if not site_pivot.empty:
            try:
                format_excel_sheet(site_pivot, writer, 'Site_Monthly_Trend')
                print("   ✅ 'Site_Monthly_Trend' sheet created.")
            except Exception as e:
                print(f"   ⚠️ Could not create 'Site_Monthly_Trend' sheet: {e}")
    
    # 3. 통합 분석
    flow_summary, monthly_trend, supplier_performance = integrated_analyzer.get_integrated_analysis()
    
    if not flow_summary.empty:
        format_excel_sheet(flow_summary, writer, 'Integrated_Flow_Analysis')
        print("   ✅ 'Integrated_Flow_Analysis' sheet created.")
    
    if not monthly_trend.empty:
        format_excel_sheet(monthly_trend, writer, 'Monthly_Trend_Overall')
        print("   ✅ 'Monthly_Trend_Overall' sheet created.")
    
    if not supplier_performance.empty:
        format_excel_sheet(supplier_performance, writer, 'Supplier_Performance')
        print("   ✅ 'Supplier_Performance' sheet created.")

def create_comprehensive_dashboard(warehouse_analyzer, site_analyzer, integrated_analyzer, delivery_tracker):
    """종합 대시보드 데이터 생성"""
    
    # 기본 통계
    total_cases = len(delivery_tracker.deliveries)
    
    # 창고별 통계
    warehouse_summary, _ = warehouse_analyzer.get_monthly_warehouse_summary()
    warehouse_stats = {}
    if not warehouse_summary.empty:
        warehouse_stats = warehouse_summary.groupby('Warehouse').agg({
            'Monthly_Inbound': 'sum',
            'Monthly_SQM_Inbound': 'sum',
            'Cases_Count': 'sum'
        }).to_dict()
    
    # 현장별 통계
    site_summary, _ = site_analyzer.get_monthly_site_summary()
    site_stats = {}
    if not site_summary.empty:
        site_stats = site_summary.groupby('Site').agg({
            'Monthly_Delivery': 'sum',
            'Monthly_SQM_Delivery': 'sum',
            'Cases_Count': 'sum'
        }).to_dict()
    
    # 통합 분석 통계
    flow_summary, monthly_trend, supplier_performance = integrated_analyzer.get_integrated_analysis()
    
    dashboard_data = {
        'Analysis_Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Total_Cases_Processed': total_cases,
        'Unique_Warehouses': len(warehouse_stats) if warehouse_stats else 0,
        'Unique_Sites': len(site_stats) if site_stats else 0,
        'Analysis_Period': f"{monthly_trend['Month'].min()} to {monthly_trend['Month'].max()}" if not monthly_trend.empty else 'N/A',
        'Avg_Lead_Time_Days': monthly_trend['Avg_Monthly_Lead_Time'].mean() if not monthly_trend.empty else 0,
        'Total_Monthly_Quantity': monthly_trend['Monthly_Quantity'].sum() if not monthly_trend.empty else 0,
        'Total_Monthly_SQM': monthly_trend['Monthly_SQM'].sum() if not monthly_trend.empty else 0
    }
    
    return pd.DataFrame([dashboard_data])

def main():
    """메인 분석 실행 함수"""
    print("🚀 Starting HVDC Comprehensive Warehouse Analysis...")
    print("📋 Target: 창고별/현장별 월별 분석 + 통합 대시보드")
    
    # 파일 존재 확인 및 자동 복사 시도
    if not check_and_copy_files():
        print("\n❌ Required files are missing. Please copy files manually and try again.")
        return
    
    # 모든 분석기 초기화
    delivery_tracker = WarehouseToSiteTracker()
    warehouse_analyzer = MonthlyWarehouseAnalyzer()
    site_analyzer = MonthlySiteAnalyzer()
    integrated_analyzer = IntegratedAnalyzer()
    all_raw_data = []
    
    # Movement 파일들 처리
    for supplier, path in FILE_MAP.items():
        print(f"   - Processing: {supplier} ({path})")
        sheet_name = SHEET_NAME_MAP.get(supplier, 'CASE LIST')
        raw_df = process_movement_file(
            path, supplier, WAREHOUSE_COLS_MAP[supplier], sheet_name, 
            delivery_tracker, warehouse_analyzer, site_analyzer, integrated_analyzer
        )
        if raw_df is not None:
            all_raw_data.append(raw_df)
    
    if not all_raw_data:
        print("⚠️ No movement data processed. Aborting.")
        return
    
    print(f"\n📊 Processing {len(delivery_tracker.deliveries)} delivery records...")
    
    # 기존 창고→현장 배송 요약 생성
    warehouse_to_site_df = delivery_tracker.get_warehouse_to_site_summary()
    
    if warehouse_to_site_df.empty:
        print("⚠️ No warehouse-to-site deliveries found.")
        return
    
    print("   - Warehouse-to-Site delivery summary generated.")
    
    # 기존 출력 (콘솔)
    print("\n📋 WAREHOUSE-TO-SITE DELIVERY SUMMARY:")
    print("=" * 50)
    
    for location in warehouse_to_site_df['Location'].unique():
        location_data = warehouse_to_site_df[warehouse_to_site_df['Location'] == location]
        print(f"\n{location}:")
        
        total_qty = 0
        total_sqm = 0
        
        for _, row in location_data.iterrows():
            print(f"  {row['Site']}: {row['Box Qty']:,} boxes, {row['SQM']:,.2f} SQM")
            total_qty += row['Box Qty']
            total_sqm += row['SQM']
        
        print(f"  Subtotal: {total_qty:,} boxes, {total_sqm:,.2f} SQM")
    
    grand_total_qty = warehouse_to_site_df['Box Qty'].sum()
    grand_total_sqm = warehouse_to_site_df['SQM'].sum()
    print(f"\nGrand Total: {grand_total_qty:,} boxes, {grand_total_sqm:,.2f} SQM")
    
    # 종합 대시보드 생성
    dashboard_df = create_comprehensive_dashboard(
        warehouse_analyzer, site_analyzer, integrated_analyzer, delivery_tracker
    )
    
    # 엑셀 파일 출력 (확장된 버전)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"HVDC_Comprehensive_Analysis_{timestamp}.xlsx"
    output_path = os.path.join(outputs_dir, output_filename)
    
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        # 📊 대시보드 시트
        if not dashboard_df.empty:
            format_excel_sheet(dashboard_df, writer, 'Dashboard')
            print("   ✅ 'Dashboard' sheet created.")
        
        # 📋 기존 시트들
        format_excel_sheet(warehouse_to_site_df, writer, 'Warehouse_to_Site_Summary')
        print("   ✅ 'Warehouse_to_Site_Summary' sheet created.")
        
        if delivery_tracker.deliveries:
            detailed_df = pd.DataFrame(delivery_tracker.deliveries)
            format_excel_sheet(detailed_df, writer, 'Detailed_Delivery_Records')
            print("   ✅ 'Detailed_Delivery_Records' sheet created.")
        
        # 🎯 새로운 월별 분석 시트들
        create_monthly_analysis_sheets(warehouse_analyzer, site_analyzer, integrated_analyzer, writer)
        
        # 📊 원본 데이터 (참고용)
        if all_raw_data:
            master_df = pd.concat(all_raw_data, ignore_index=True, sort=False)
            format_excel_sheet(master_df, writer, 'Raw_Movement_Data')
            print("   ✅ 'Raw_Movement_Data' sheet created.")
    
    print(f"\n📦 Comprehensive Report saved to: '{output_path}'")
    
    # 📊 월별 분석 요약 출력
    print("\n📊 MONTHLY ANALYSIS SUMMARY:")
    print("=" * 50)
    
    # 창고별 월별 요약
    warehouse_summary, _ = warehouse_analyzer.get_monthly_warehouse_summary()
    if not warehouse_summary.empty:
        print("\n🏢 창고별 월별 입고 현황:")
        warehouse_monthly = warehouse_summary.groupby('Warehouse').agg({
            'Monthly_Inbound': 'sum',
            'Monthly_SQM_Inbound': 'sum',
            'Cases_Count': 'sum'
        })
        for warehouse in warehouse_monthly.index:
            data = warehouse_monthly.loc[warehouse]
            print(f"  {warehouse}: {data['Monthly_Inbound']:,} boxes, {data['Monthly_SQM_Inbound']:,.2f} SQM, {data['Cases_Count']:,} cases")
    
    # 현장별 월별 요약
    site_summary, _ = site_analyzer.get_monthly_site_summary()
    if not site_summary.empty:
        print("\n🏗️ 현장별 월별 배송 현황:")
        site_monthly = site_summary.groupby('Site').agg({
            'Monthly_Delivery': 'sum',
            'Monthly_SQM_Delivery': 'sum',
            'Cases_Count': 'sum'
        })
        for site in site_monthly.index:
            data = site_monthly.loc[site]
            print(f"  {site}: {data['Monthly_Delivery']:,} boxes, {data['Monthly_SQM_Delivery']:,.2f} SQM, {data['Cases_Count']:,} cases")
    
    # 통합 분석 요약
    flow_summary, monthly_trend, supplier_performance = integrated_analyzer.get_integrated_analysis()
    if not monthly_trend.empty:
        print("\n📈 월별 전체 처리량 트렌드:")
        for _, row in monthly_trend.iterrows():
            print(f"  {row['Month']}: {row['Monthly_Quantity']:,} boxes, {row['Monthly_SQM']:,.2f} SQM, 평균 리드타임 {row['Avg_Monthly_Lead_Time']:.1f}일")
    
    if not supplier_performance.empty:
        print("\n🏭 공급사별 성과:")
        for _, row in supplier_performance.iterrows():
            print(f"  {row['Supplier']}: {row['Total_Quantity']:,} boxes, 평균 리드타임 {row['Avg_Lead_Time']:.1f}일")
    
    print("\n🎯 Expected vs Actual Comparison:")
    print("   - DSV AL MARKAZ 예상: 812 boxes, 4,856.87 SQM")
    print("   - DSV Indoor 예상: 414 boxes, 2,458.27 SQM")
    print("   - Grand Total 예상: 1,226 boxes, 7,315.14 SQM")
    print("\n📋 Generated Analysis Sheets:")
    print("   1. Dashboard - 종합 개요")
    print("   2. Warehouse_to_Site_Summary - 창고→현장 배송 요약")
    print("   3. Monthly_Warehouse_Summary - 창고별 월별 입고/재고")
    print("   4. Monthly_Site_Summary - 현장별 월별 입고/재고")
    print("   5. Integrated_Flow_Analysis - 통합 흐름 분석")
    print("   6. Monthly_Trend_Overall - 전체 월별 트렌드")
    print("   7. Supplier_Performance - 공급사별 성과")
    print("   8. Detailed_Delivery_Records - 상세 배송 기록")
    print("   9. Raw_Movement_Data - 원본 데이터")
    
    try:
        if os.name == 'nt': 
            subprocess.run(['start', output_path], shell=True, check=True)
        else: 
            os.system(f"open '{output_path}'")
    except Exception as e:
        print(f"⚠️ Could not open file automatically: {e}")


if __name__ == '__main__':
    main()