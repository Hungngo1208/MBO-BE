import pandas as pd

file_path = r"C:\Users\ADMIN\Downloads\s.xlsx"
df = pd.read_excel(file_path)

# Lấy cột đầu tiên (cột A, index = 0)
df_unique = df.drop_duplicates(subset=[df.columns[0]])

output_path = r"C:\Users\ADMIN\Downloads\duplicates.xlsx"
df_unique.to_excel(output_path, index=False)

print("Đã lưu file:", output_path)
