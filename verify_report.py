import pandas as pd
import glob
import os
from datetime import datetime

def verify_and_create_proper_report():
    """Verify each HVDC file and create a proper report with separate sheets"""
    
    # Get all Excel files in the current directory
    files = glob.glob('*.xlsx')
    print(f"Found {len(files)} Excel files:")
    
    # Filter out generated report files
    files = [f for f in files if not f.startswith('HVDC_') or not ('Report' in f or 'Proper' in f)]
    print(f"Processing {len(files)} data files:")
    
    if not files:
        print("❌ No Excel data files found in current directory")
        print("Expected files:")
        print("   - HVDC WAREHOUSE_HITACHI(HE).xlsx")
        print("   - HVDC WAREHOUSE_HITACHI(HE-0214,0252)1.xlsx")
        print("   - HVDC WAREHOUSE_HITACHI(HE_LOCAL).xlsx")
        print("   - HVDC WAREHOUSE_SIMENSE(SIM).xlsx")
        return []
    
    # Analyze each file
    file_info = []
    
    # Only create Excel writer if we have files to process
    if files:
        with pd.ExcelWriter('HVDC_Proper_Report.xlsx', engine='openpyxl') as writer:
            for file in files:
                try:
                    df = pd.read_excel(file)
                    filename = os.path.basename(file)
                    sheet_name = filename.replace('.xlsx', '')[:30]  # Excel sheet name limit
                    
                    # Save to separate sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Record file info
                    file_info.append({
                        'File': filename,
                        'Rows': len(df),
                        'Columns': len(df.columns),
                        'Size_KB': round(os.path.getsize(file) / 1024, 1)
                    })
                    
                    print(f"✓ {filename}: {len(df)} rows, {len(df.columns)} columns")
                    
                except Exception as e:
                    print(f"✗ Error reading {file}: {e}")
            
            # Create summary sheet
            if file_info:
                summary_df = pd.DataFrame(file_info)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"\n✅ Proper report created: HVDC_Proper_Report.xlsx")
    print(f"📊 Total files processed: {len(file_info)}")
    
    return file_info

if __name__ == "__main__":
    verify_and_create_proper_report() 