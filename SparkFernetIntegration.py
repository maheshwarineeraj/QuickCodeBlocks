# Details - https://maheshwarineeraj.medium.com/secure-data-ingestion-with-spark-and-fernet-e7fc2100a8e8 

"""

env Setup
pip install Fernet
pip install pyspark

"""

from cryptography.fernet import Fernet
from pyspark.sql.functions import *
from pyspark.sql.types import *


# encrypts plain text
def encryptor(text, encryption_key):
  
  f = Fernet(encryption_key)
  text_bytes = bytes(text, 'utf-8')
  encrypted_text = f.encrypt(text_bytes)
  encrypted_text = str(encrypted_text.decode('ascii'))
  
  return encrypted_text

# register encryptor to Spark UDF
encryptify = udf(encryptor, StringType())


# this function encrypt the text values for specified columns in col_list in new column with _encrypted sufix and drop original col
def data_shield(df, encryption_key, col_list):
    for column in col_list:
        df = df.withColumn(f"{column}_encrypted", encryptify(column, lit(encryption_key))).drop(column)
        
    return df

# load data from source, in this example creating dummy dataframe
df = spark \
  .createDataFrame([[1, "abc", "450"],
          [2, "def", "784"],
          [3, "ghi", "200"],
          [4, "abc","800"],
          [5, "jkl", "850"],
          [6, "def", "745"]],
      schema = ["user_id", "name", "credit_score"])

# encrypt single column
df_encrypted_1 = data_shield(df, key, ["name"])
df_encrypted_1.show()

# encrypt multiple columns
df_encrypted_2 = data_shield(df, key, ["name", "credit_score"])
df_encrypted_2.show()
