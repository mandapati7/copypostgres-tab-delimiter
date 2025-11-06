import csv

# Configuration
num_rows = 1_000_000
num_cols = 10
output_file = f'{num_rows}_large_test.csv'
columns = [f'col{i+1}' for i in range(num_cols)]

# Generate CSV
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(columns)
    for row_num in range(1, num_rows + 1):
        row = [f'data_{col}_{row_num}' for col in range(1, num_cols + 1)]
        writer.writerow(row)

print(f'CSV file "{output_file}" with {num_rows} rows and {num_cols} columns generated successfully.')

# Generate marker done file
with open(f'{output_file}.done', 'w') as donefile:
    donefile.write('CSV generation completed.\n')

