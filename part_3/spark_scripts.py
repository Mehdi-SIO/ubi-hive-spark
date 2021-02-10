from pyspark.sql.functions import length, when

#command to generate 'amount_spent_ten_last_min' column
firstdf = schemaTransaction\
    .orderBy(col("customerid").asc(), col("order_datetime").asc())\
    .select("customerid", "amount_eur",'order_datetime')

my_window = Window.partitionBy().orderBy("customerid")

newdf = parisdf\
    .withColumn("prev_value", lag(parisdf.order_datetime).over(my_window))\
    .withColumn("prev_id", lag(parisdf.customerid).over(my_window))\
    .withColumn("prev_amount", lag(parisdf.amount_eur).over(my_window))\
    .withColumn("date_diff", datediff(col("order_datetime"), col("prev_value")))\
    .withColumn("amount_spent_ten_last_min", when((0 < col("date_diff")) & (col("date_diff") < 600) & (col("customerid") == col("prev_id")), col("prev_amount")).otherwise(0))
newdf.show(100)