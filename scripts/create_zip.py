import zipfile
import os
import glob

def create_zip_excluding_excel():
    """Create ZIP file excluding Excel files"""
    
    zip_filename = "HVDC_Analysis_Pipeline.zip"
    
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add Python files in root directory
        for file in glob.glob("*.py"):
            if not file.endswith('.xlsx'):
                zipf.write(file)
                print(f"Added: {file}")
        
        # Add analytics directory (excluding Excel files)
        for root, dirs, files in os.walk("analytics"):
            for file in files:
                if not file.endswith('.xlsx'):
                    file_path = os.path.join(root, file)
                    zipf.write(file_path)
                    print(f"Added: {file_path}")
    
    print(f"\n✅ ZIP file created: {zip_filename}")

if __name__ == "__main__":
    create_zip_excluding_excel() 