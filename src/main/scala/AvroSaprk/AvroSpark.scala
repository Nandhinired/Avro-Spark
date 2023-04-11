package AvroSaprk

import org.apache.spark.sql.SparkSession

object AvroSpark extends App{

  override def main(arge:Array[String]) :Unit = {

    val spark = SparkSession.builder()
      .appName("Avro Data")
      .master("local[3]")
      .getOrCreate()

    val input = spark.read.parquet("C:/Users/nandh/Desktop/BigData/parquet_output/part-00000-16718f6d-5547-426b-8bf9-b6c795cc215d-c000.snappy.parquet")

    input.show(3)

    input.write
      .format("avro")
      .mode("overwrite")
      .save("C:/Users/nandh/Desktop/BigData/Avro_Output")
  }

}
