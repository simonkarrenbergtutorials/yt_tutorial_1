%%pyspark
blob_account_name = "sadatevtopbi"
blob_container_name = "config"
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.window import Window

sc = SparkSession.builder.getOrCreate()
token_library = sc._jvm.com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
blob_sas_token = token_library.getConnectionString("AzureBlobStorage1")

spark.conf.set(
    'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
    blob_sas_token)
df = spark.read.option("header", "true").load('wasbs://config@sadatevtopbi.blob.core.windows.net/BWA_Schema.csv', format='csv'
## If header exists uncomment line below
##, header=True
)
selected_cols = df.columns[:2]
df = df.select(*selected_cols)

display(df)


df = df.toPandas()

for index, row in df.iterrows():
    if len(row['Zeile'].strip()) == 0:
        df.at[index,'isEmpty'] = True
    else:
        df.at[index,'isEmpty'] = False

df_pnl_lines = df[df["isEmpty"] == False]

display(df_pnl_lines.sort_values(by='Zeile', inplace=True))

for index, row in df.iterrows():
    if len(row['Zeile'].strip()) > 0:
        row_saved = row['Zeile']
    elif len(row['Zeile'].strip()) == 0:
        df.at[index,'Zeile'] = row_saved

df = df[df["isEmpty"] == True]

df = pd.merge(df, df_pnl_lines, on='Zeile', how='left')

df[['Konto', 'Konto Text']] = df['Bezeichnung_x'].str.split(n=1, expand=True)

df = df.rename(columns={'Bezeichnung_y': 'P&L Line'})

df = df.sort_values(by='Zeile')
df['Rank'] = (df['Zeile'] != df['Zeile'].shift(1)).astype(int).cumsum().astype(str)
df['Rank'] = df['Rank'].apply('{:0>2}'.format)
df["P&L Line"] = df['Rank'] + '_' + df['P&L Line'].astype(str)

display(df)

df = df[["P&L Line", "Konto", "Konto Text"]]
df = spark.createDataFrame(df)
df.coalesce(1).write.csv('wasbs://config@sadatevtopbi.blob.core.windows.net/BWA_Schema_clean.csv', mode='overwrite', header=True)
