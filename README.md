# HVDC Analysis Pipeline

HVDC (High Voltage Direct Current) 창고 데이터 분석을 위한 완전 자동화 파이프라인입니다.

## 📋 프로젝트 개요

이 프로젝트는 HVDC 프로젝트의 창고 운영 데이터를 분석하여 재고 관리, 배송 추적, 비용 분석 등을 수행하는 종합적인 시스템입니다.

## 🚀 주요 기능

### 1. 데이터 정규화 (Ontology 기반)
- 창고명, 현장명, 카테고리 자동 매핑
- 다양한 Excel 파일 형식 자동 인식
- 퍼지 매칭을 통한 컬럼 자동 탐지

### 2. 재고 계산 엔진
- 일별 재고: Opening/Inbound/Outbound/Closing
- 월별 집계: 창고별/현장별 통계
- 재고 차이 분석: 트랜잭션 vs 실제 재고

### 3. 고급 분석 기능
- 창고별 월별 입출고 분석
- 현장별 배송 분석
- 통합 창고-현장 흐름 분석
- 비용 분석 및 KPI 대시보드

### 4. 자동 리포트 생성
- 15개 이상의 시트로 구성된 종합 Excel 리포트
- 실시간 KPI 대시보드
- 상세한 분석 결과

## 📁 파일 구조

```
HVDC_Analysis_Pipeline/
├── HVDC analysis.py          # 메인 분석 엔진 (47KB, 1159줄)
├── analysis.py               # 이전 버전 분석 스크립트 (41KB, 1003줄)
├── create_zip.py             # 패키징 유틸리티
├── verify_report.py          # 리포트 검증 도구
├── analytics/
│   ├── simple_analysis.py    # 단순 분석 스크립트
│   └── run_wh_analysis.py    # 창고 분석 실행기
├── *.xlsx                    # HVDC 데이터 파일들
└── README.md                 # 프로젝트 문서
```

## 🔧 설치 및 실행

### 필수 패키지
```bash
pip install pandas openpyxl xlsxwriter numpy
```

### 실행 방법
```bash
python "HVDC analysis.py"
```

## 📊 데이터 파일

### 지원하는 파일 형식
- **BL (Bill of Lading)**: 히타치/지멘스 메인 데이터
- **MIR (Material Issue Request)**: 로컬 데이터
- **PICK**: 특정 로트 데이터
- **INVOICE**: 비용 데이터
- **ONHAND**: 재고 스냅샷

### 현재 데이터 파일
- `HVDC WAREHOUSE_HITACHI(HE).xlsx` (824KB) - 히타치 메인 데이터
- `HVDC WAREHOUSE_HITACHI(HE-0214,0252)1.xlsx` (23KB) - 히타치 특정 로트
- `HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx` (16KB) - 히타치 로컬 데이터
- `HVDC WAREHOUSE_SIMENSE(SIM).xlsx` (399KB) - 지멘스 데이터

## 📈 분석 결과물

### 생성되는 Excel 리포트 (`HVDC_Comprehensive_Report.xlsx`)

1. **📊 Dashboard** - 종합 KPI 대시보드
2. **🏢 Warehouse Monthly** - 창고별 월별 입출고 분석
3. **🏗️ Site Delivery** - 현장별 배송 분석
4. **🔄 Integrated Flow** - 창고↔현장 흐름 분석
5. **💰 Cost Analysis** - 비용 분석
6. **📅 일별재고추적** - 일별 상세 재고 변동
7. **⚖️ 재고차이분석** - Tx vs OnHand 차이
8. **📄 원본데이터** - 전체 트랜잭션 데이터

## 🏢 창고 Ontology

### 창고 매핑 규칙
```python
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
```

### 현장 매핑 규칙
```python
SITE_PATTERNS = {
    r".*AGI.*": "AGI",
    r".*DAS.*": "DAS", 
    r".*MIR.*": "MIR",
    r".*SHU.*": "SHU"
}
```

## 🔄 데이터 흐름

```
Excel 파일들 → 자동 탐지 → Ontology 매핑 → 재고 계산 → 고급 분석 → Excel 리포트
```

## ✨ 프로젝트 특징

1. **완전 자동화**: 파일 자동 탐지 → 정규화 → 분석 → 리포트 생성
2. **확장성**: 새로운 창고/현장 추가 시 Ontology만 업데이트
3. **정확성**: 재고 차이 분석으로 데이터 품질 검증
4. **사용자 친화적**: 이모지와 한글을 활용한 직관적인 출력
5. **엔터프라이즈급**: 대용량 데이터 처리 및 복잡한 비즈니스 로직

## 🛠️ 개발 정보

- **언어**: Python 3.7+
- **주요 라이브러리**: pandas, openpyxl, xlsxwriter, numpy
- **아키텍처**: 모듈화된 클래스 기반 설계
- **성능**: 대용량 Excel 파일 처리 최적화

## 📝 라이선스

이 프로젝트는 HVDC 프로젝트 전용으로 개발되었습니다.

## 🤝 기여

프로젝트 개선을 위한 제안사항이 있으시면 언제든지 연락주세요.

---

**개발자**: HVDC Analysis Team  
**최종 업데이트**: 2024년 