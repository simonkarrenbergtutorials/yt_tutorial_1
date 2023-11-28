import pandas as pd

# Assuming your original data is in an Excel file named 'test_datev.xlsx'
excel_path = r'C:\Users\xxx.xls'

# Read the data with headers
df = pd.read_excel(excel_path, header=None)

df = df.iloc[:, :3]

filtered_df = df[df[0].apply(lambda x: isinstance(x, (int, float)))]

# Iterate over rows and fill empty cells in the first column
current_value = None
for index, row in df.iterrows():
    if row[0] == '  ':
        df.at[index, 0] = current_value
    else:
        current_value = row[0]

# Drop rows with missing values in the second column
df = df.dropna(subset=[df.columns[1]])

# Reset index
df = df.reset_index(drop=True)

# Merge the original DataFrame and the filtered DataFrame on the first column
merged_df = pd.merge(df, filtered_df, on=0, how='inner')

# Filter out rows where the values in the second and third columns are equal
final_df = merged_df[merged_df[merged_df.columns[1]] != merged_df[merged_df.columns[3]]]

final_df = final_df.iloc[:, [0,1, 3]]

# Save the final DataFrame to a new Excel file named 'output_final.xlsx'
output_final_excel_path = r'C:\Users\SimonKarrenberg\Desktop\output_final.xlsx'
final_df.to_excel(output_final_excel_path, index=False, header=False)
