from pyspark.sql.functions import length, when

#command to generate 'nb_distinct_char_in_address' column
schemaTransaction.select(length('address').alias('nb_distinct_char_in_address'))

#command to genereate 'is_located_in_rue_de_paris' column
from pyspark.sql.functions import length, when, col, lower

schemaTransaction\
    .withColumn('is_located_in_rue_de_paris', when(lower(col("address")).contains('rue de paris'), True).otherwise(False))\
    .select('is_located_in_rue_de_paris')

#command to generate and show asked dataframe combining all columns

schemaTransaction\
    .withColumn('nb_distinct_char_in_address', length('address'))\
    .withColumn('is_located_in_rue_de_paris', when(lower(col("address")).contains('rue de paris'), True).otherwise(False))\
    .show()