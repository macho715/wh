# corrected_warehouse_monthly_logic.py
"""
올바른 창고별 월별 입출고/재고 계산 로직
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any

class CorrectedStockEngine:
    """수정된 재고 계산 엔진"""
    
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

# 검증 실행 함수
def run_stock_logic_validation(df: pd.DataFrame):
    """재고 로직 검증 실행"""
    
    print("🔍 창고별 월별 입출고 재고 로직 검증 시작...")
    
    # 수정된 로직 적용
    corrected_results = CorrectedStockEngine.create_proper_monthly_warehouse_analysis(df)
    
    if not corrected_results:
        print("❌ 분석할 데이터가 없습니다.")
        return
    
    # 검증 실행
    validation = CorrectedStockEngine.validate_stock_logic(corrected_results)
    
    print(f"\n📊 검증 결과:")
    print(f"   ✅ 검증 통과: {'예' if validation['validation_passed'] else '아니오'}")
    print(f"   📋 검사 대상: {validation['total_records_checked']}개 기록")
    print(f"   🏢 창고 수: {validation['locations_checked']}개")
    print(f"   📅 월 수: {validation['months_checked']}개")
    
    if validation['errors']:
        print(f"\n❌ 발견된 오류 ({len(validation['errors'])}개):")
        for error in validation['errors'][:5]:  # 최대 5개만 표시
            print(f"   - {error}")
        if len(validation['errors']) > 5:
            print(f"   ... 및 {len(validation['errors']) - 5}개 추가 오류")
    
    if validation['warnings']:
        print(f"\n⚠️ 경고사항 ({len(validation['warnings'])}개):")
        for warning in validation['warnings']:
            print(f"   - {warning}")
    
    # 샘플 데이터 출력
    if 'monthly_stock_detail' in corrected_results:
        sample = corrected_results['monthly_stock_detail'].head(10)
        print(f"\n📋 월별 재고 상세 (샘플 10개):")
        print(sample[['Location', 'YearMonth', 'Opening_Stock', 'Inbound_Qty', 'Outbound_Qty', 'Closing_Stock']].to_string(index=False))
    
    return corrected_results, validation

# 기존 코드 vs 수정된 코드 비교
def compare_old_vs_new_logic():
    """기존 로직과 수정된 로직 비교"""
    
    comparison = {
        '항목': [
            '재고 계산 방식',
            '월별 집계 방식', 
            '출고 처리',
            '연속성 보장',
            '검증 기능',
            '음수 재고 처리',
            '피벗 테이블'
        ],
        '기존 로직 (문제점)': [
            'Opening = (누적입고-누적출고).shift(1) ❌',
            '케이스 최종 상태만 사용 ❌',
            '출고 로직 불완전 ❌',
            '보장되지 않음 ❌',
            '없음 ❌',
            '처리되지 않음 ❌',
            'MultiIndex 오류 발생 ❌'
        ],
        '수정된 로직 (개선점)': [
            'Opening = 전월 Closing ✅',
            '실제 월별 입출고 이벤트 추적 ✅',
            '창고→사이트 출고 명확히 구분 ✅',
            '월간 연속성 자동 검증 ✅',
            '완전한 검증 함수 제공 ✅',
            '음수 재고 감지 및 경고 ✅',
            '안정적인 피벗 테이블 생성 ✅'
        ]
    }
    
    return pd.DataFrame(comparison)

if __name__ == "__main__":
    # 비교표 출력
    comparison_df = compare_old_vs_new_logic()
    print("🔄 기존 로직 vs 수정된 로직 비교:")
    print("=" * 80)
    print(comparison_df.to_string(index=False))
    
    print("\n💡 권장사항:")
    print("1. 기존 HVDC analysis.py의 StockEngine 클래스를 위 수정된 로직으로 교체")
    print("2. 검증 함수를 추가하여 데이터 무결성 확인")
    print("3. 음수 재고 발생 시 비즈니스 규칙 정의")
    print("4. 출고 이벤트 추적 로직 강화") 