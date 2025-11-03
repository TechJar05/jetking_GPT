import os
import pandas as pd
 
# 📂 Folder containing your Excel files
FOLDER_PATH = "./data_upload"  # change this to your actual folder path
 
# To store all results
summary = []
 
print(f"\n🔍 Scanning folder: {os.path.abspath(FOLDER_PATH)}\n")
 
# Loop through all Excel files
for file in os.listdir(FOLDER_PATH):
    if file.endswith(".xlsx") or file.endswith(".xls"):
        file_path = os.path.join(FOLDER_PATH, file)
        try:
            # Read only first few rows for speed and safety
            xls = pd.ExcelFile(file_path)
            print(f"📘 File: {file}")
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)
                cols = list(df.columns)
                print(f"  📄 Sheet: {sheet_name}")
                print(f"     Columns ({len(cols)}): {cols}\n")
                summary.append({
                    "file": file,
                    "sheet": sheet_name,
                    "columns": ", ".join(cols)
                })
        except Exception as e:
            print(f"⚠️ Error reading {file}: {e}")
 
# Optional: Save column summary to CSV
output_file = "excel_column_summary.csv"
pd.DataFrame(summary).to_csv(output_file, index=False)
print(f"\n✅ Column summary saved to: {output_file}")