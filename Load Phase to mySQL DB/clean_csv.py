import pandas as pd

transactions = pd.read_csv('Fetch-Data-Analyst-Take-Home/Original Files/TRANSACTION_TAKEHOME.csv', encoding='utf-8', dtype={'BARCODE': object,'MANUFACTURER': object}, na_values=['NaN','nan'])
products = pd.read_csv('Fetch-Data-Analyst-Take-Home/Original Files/PRODUCTS_TAKEHOME.csv', encoding='utf-8', dtype={'BARCODE': object}, na_values=['NaN','nan'])
users = pd.read_csv('Fetch-Data-Analyst-Take-Home/Original Files/USER_TAKEHOME.csv', encoding='utf-8')

# Clean up For SQL Work
## Products
#   fill barcode nulls
# print(products.info()) # Check if dtypes are expected
# print(products.dtypes)
products.to_csv('Fetch-Data-Analyst-Take-Home/Cleaned Files/PRODUCTS.csv', encoding='utf-8',mode='w',index=False)
print('PRODUCTS.csv complete')

# Users :
#   Remove Z from time_stamps; Convert to MDYYYY
users['CREATED_DATE'] = users['CREATED_DATE'].str.replace(' Z','')
users['BIRTH_DATE'] = users['BIRTH_DATE'].str.replace(' Z','')
users['CREATED_DATE'] = pd.to_datetime(users['CREATED_DATE'])
users['BIRTH_DATE'] = pd.to_datetime(users['BIRTH_DATE'])
# print(users.info())
users.to_csv('Fetch-Data-Analyst-Take-Home/Cleaned Files/USERS.csv', encoding='utf-8',mode='w',index=False)
print('USERS.csv complete')

## Transactions:
# print(transactions)
# remove Z from timestamps; convert to MDYYYY
transactions['SCAN_DATE'] = transactions['SCAN_DATE'].str.replace(' Z','')
transactions['SCAN_DATE'] = pd.to_datetime(transactions['SCAN_DATE'])
transactions['PURCHASE_DATE'] = pd.to_datetime(transactions['PURCHASE_DATE'])
# replace 'zero' from final quantity
transactions['FINAL_QUANTITY'] = transactions['FINAL_QUANTITY'].str.replace('zero','0.00').astype(float)
# replace whitespace
transactions['FINAL_SALE'] = transactions['FINAL_SALE'].str.replace(' ','0').astype(float)
# print(transactions.info())
transactions.to_csv('Fetch-Data-Analyst-Take-Home/Cleaned Files/TRANSACTIONS.csv', encoding='utf-8',mode='w',index=False)
print('TRANSACTIONS.csv complete')