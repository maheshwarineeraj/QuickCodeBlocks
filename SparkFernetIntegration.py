# Details - 


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
