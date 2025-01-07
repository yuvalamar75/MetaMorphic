import pandas as pd
import os

# Define the save directory
save_dir = '/Users/yuval/MetaMorphic/sampels/batch_1'

# Create the directory if it doesn't exist
os.makedirs(save_dir, exist_ok=True)
print(f"Files will be saved to: {save_dir}")

# Define base IDs
base_ids = ['12345678', '23456789', '34567890']

# Function to transform base IDs into different formats
def transform_ids(base_ids):
    transformed = {
        'file1': [],  # תעודת זהות with dash before last digit
        'file2': [],  # תז with 0 prefix
        'file3': [],  # id with 0 prefix and dash before last digit
        'file4': [],  # תעודת זהות with 9-digit number
        'file5': []   # id with mixed formats
    }
    
    for id in base_ids:
        # File 1: Numbers with a dash before the last digit (e.g., 1234567-8)
        file1_id = f"{id[:-1]}-{id[-1]}"
        transformed['file1'].append(file1_id)
        
        # File 2: Number with a '0' prefix (e.g., 012345678)
        file2_id = f"0{id}"
        transformed['file2'].append(file2_id)
        
        # File 3: Number with a '0' prefix and a dash before the last digit (e.g., 0123456-8)
        file3_id = f"0{id[:-1]}-{id[-1]}"
        transformed['file3'].append(file3_id)
        
        # File 4: 9-digit number (assuming leading zero if necessary)
        file4_id = f"0{id}" if len(id) == 8 else id  # Ensure 9 digits
        transformed['file4'].append(file4_id)
        
    return transformed

# Get transformed IDs
transformed_ids = transform_ids(base_ids)

# Helper function to construct full file paths
def get_file_path(filename):
    return os.path.join(save_dir, filename)

# File 1: file1.xlsx
# Key Column Name: תעודת זהות
# Key Column Format: Numbers with a dash before the last digit (e.g., 1234567-8)
file1_data = {
    'תעודת זהות': transformed_ids['file1'],
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [30, 25, 28]
}

df1 = pd.DataFrame(file1_data)
file1_path = get_file_path('file1.xlsx')
df1.to_excel(file1_path, index=False, engine='openpyxl')
print(f"file1.xlsx created successfully at {file1_path}")

# File 2: file2.xlsx
# Key Column Name: תז
# Key Column Format: Number with a '0' prefix (e.g., 012345678)
file2_data = {
    'תז': transformed_ids['file2'],
    'Department': ['HR', 'IT', 'Marketing'],
    'Salary': [50000, 60000, 55000]
}

df2 = pd.DataFrame(file2_data)
file2_path = get_file_path('file2.xlsx')
df2.to_excel(file2_path, index=False, engine='openpyxl')
print(f"file2.xlsx created successfully at {file2_path}")

# File 3: file3.csv
# Key Column Name: id
# Key Column Format: Number with a '0' prefix and dash before the last digit (e.g., 0123456-8)
file3_data = {
    'id': transformed_ids['file3'],
    'Project': ['Alpha', 'Beta', 'Gamma'],
    'StartDate': ['2024-01-15', '2024-02-20', '2024-03-10']
}

df3 = pd.DataFrame(file3_data)
file3_path = get_file_path('file3.csv')
df3.to_csv(file3_path, index=False, encoding='utf-8-sig')
print(f"file3.csv created successfully at {file3_path}")

# File 4: file4.csv
# Key Column Name: תעודת זהות
# Key Column Format: 9-digit number (e.g., 012345678)
file4_data = {
    'תעודת זהות': transformed_ids['file4'],
    'Location': ['New York', 'Los Angeles', 'Chicago'],
    'Email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
}

df4 = pd.DataFrame(file4_data)
file4_path = get_file_path('file4.csv')
df4.to_csv(file4_path, index=False, encoding='utf-8-sig')
print(f"file4.csv created successfully at {file4_path}")

# File 5: file5.csv
# Key Column Name: id
# Key Column Format: Mixed formats
file5_data = {
    'id': [
        transformed_ids['file1'][0],  # 1234567-8
        transformed_ids['file2'][1],  # 023456789
        transformed_ids['file3'][2],  # 0345678-0
        transformed_ids['file4'][0]   # 012345678
    ],
    'Role': ['Manager', 'Developer', 'Designer', 'Analyst'],
    'Status': ['Active', 'Inactive', 'Active', 'Active']
}

df5 = pd.DataFrame(file5_data)
file5_path = get_file_path('file5.csv')
df5.to_csv(file5_path, index=False, encoding='utf-8-sig')
print(f"file5.csv created successfully at {file5_path}")

print("\nAll files have been created successfully in the specified directory.")
