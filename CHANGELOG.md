# Changelog

모든 주요 변경사항이 이 파일에 기록됩니다.

## [1.0.0] - 2024-12-19

### 추가됨
- HVDC 창고 데이터 분석 파이프라인 초기 버전
- Ontology 기반 데이터 정규화 시스템
- 재고 계산 엔진 (일별/월별)
- 고급 분석 기능 (창고별/현장별/통합 흐름)
- KPI 대시보드 및 Excel 리포트 생성
- 자동 파일 탐지 및 처리
- 퍼지 매칭을 통한 컬럼 자동 탐지
- 재고 차이 분석 기능

### 주요 기능
- **DataExtractor**: 다양한 Excel 파일 형식 자동 인식
- **StockEngine**: 재고 계산 및 추적
- **AdvancedAnalytics**: 고급 분석 및 KPI 계산
- **ReportWriter**: Excel 리포트 생성 및 서식 적용

### 지원 파일 형식
- BL (Bill of Lading): 히타치/지멘스 메인 데이터
- MIR (Material Issue Request): 로컬 데이터
- PICK: 특정 로트 데이터
- INVOICE: 비용 데이터
- ONHAND: 재고 스냅샷

### 창고 Ontology
- DSV Indoor, DSV Outdoor, DSV Al Markaz
- MOSB, DSV MZP, DHL WH, AAA Storage
- 현장: AGI, DAS, MIR, SHU

### 기술 스택
- Python 3.7+
- pandas, openpyxl, xlsxwriter, numpy
- 모듈화된 클래스 기반 아키텍처

---

## [Unreleased]

### 계획된 기능
- 웹 대시보드 인터페이스
- 실시간 데이터 업데이트
- API 엔드포인트
- 데이터베이스 연동
- 고급 시각화 기능 