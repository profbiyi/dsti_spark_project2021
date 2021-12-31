
import org.apache.spark.sql.DataFrame

def extractDeliveriesMoreThan10Days: DataFrame = {
    val path = "C:/Users/ibnqu/Desktop/spark_scala_stuff/olist"
    val ordersPath = s"$path/olist_orders_dataset.csv"
    val ordersDF = spark.read.option("header", "true").csv(ordersPath)
    ordersDF.createOrReplaceTempView("orders")
    spark.sql("select datediff(to_utc_timestamp(order_delivered_customer_date, 'UTC-3'), to_utc_timestamp(order_purchase_timestamp, 'UTC-3')) as delivery_delay_in_days from orders where datediff(to_utc_timestamp(order_delivered_customer_date, 'UTC-3'), to_utc_timestamp(order_purchase_timestamp, 'UTC-3')) > 10 ")
}


//show
extractDeliveriesMoreThan10Days.show

//sanityCheck
extractDeliveriesMoreThan10Days.filter(col("delivery_delay_in_days") <= 10).show
extractDeliveriesMoreThan10Days.filter(col("delivery_delay_in_days") <= 10).count == 0

//output as CSV
extractDeliveriesMoreThan10Days.coalesce(1).write.option("header", "true").csv("delivery_delay_in_days.csv")