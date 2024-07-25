import csv


file_path = "/home/sikandar/Documents/GitHub/Email-Verification-Backend/emailverify/emailverify/CSV/Phil first 15k batch_KOXYJS_Matched.csv"

with open(file_path, 'r', encoding='utf-8') as file:
    reader = csv.reader(file)
    count = sum(1 for row in reader if row and row[0].strip())
    print(count)
