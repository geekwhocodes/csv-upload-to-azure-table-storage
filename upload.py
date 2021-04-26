import pandas as pd
import numpy as np
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from azure.cosmosdb.table.tablebatch import TableBatch

table_service = TableService(account_name='', account_key='')
table_name = ''

if not table_service.exists(table_name):
    table_service.create_table(table_name, fail_on_exist=False)

batch_size = 50
# unused vars can be used to set Partition & Row key from existing column
# refer line number 23
RowKeyProp = 'bookID'
partitionKeyProp = 'authors'
# unused vars

# read csv
file_path = 'userse.csv'
df = pd.read_csv(file_path, encoding='utf-8', error_bad_lines=False)
#df.rename(columns={RowKeyProp: 'RowKey'}, inplace=True)
df = df.dropna()
df = df.applymap(str)
batch_size = np.ceil(len(df)/batch_size)
print('{} rows found. batch size {}'.format(len(df), batch_size))

data = df.to_dict(orient='records')

for chunk in np.array_split(data, batch_size):
    # create batch
    batch = TableBatch()
    for row in chunk:
        batch.insert_or_replace_entity(row)
    table_service.commit_batch(table_name, batch)

