import pandas as pd

file_path = r"C:\Users\ADMIN\Downloads\ss.xlsx"

# Đọc cả 2 cột
df = pd.read_excel(file_path)

# In ra danh sách cột để chắc chắn
print(df.columns.tolist())

# Trường hợp cột bị trùng tên, pandas sẽ đặt: ['Code', 'Code.1']
colA = 'Code'
colB = 'Code.1'

# Xử lý cột A: chuỗi có 0 ở đầu
df[colA] = df[colA].astype(str).str.strip().str.lstrip('0')
df[colA] = df[colA].replace('', '0').astype(int)

# Xử lý cột B: số
df[colB] = pd.to_numeric(df[colB], errors='coerce').astype('Int64')

# Tìm số có trong A nhưng không có trong B
missing_in_B = df.loc[~df[colA].isin(df[colB]), colA].unique()

print("Các số có trong A nhưng không có trong B:")
print(missing_in_B.tolist())
