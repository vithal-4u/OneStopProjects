package com.spark.java.poc.examples;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.api.java.JavaRDD;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DatasetTransformations {

	public static void main(String[] args) {
		// Create a session
		SparkSession spark = new SparkSession.Builder().appName("CSV to DB").master("local").getOrCreate();

		// get data
		Dataset<Row> df = spark.read().format("csv").option("header", true)
				.load("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\name_and_comments.txt");

		df.show(3);

		// Transformation
		df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")))
				.filter(df.col("comment").rlike("\\d+")).orderBy(df.col("last_name").asc()).show(false);
		
		df.withColumn("contains_the", col("comment").rlike("\\bthe\\b|\\bThe\\b")).filter(df.col("comment").rlike("\\bthe\\b|\\bThe\\b")).show(false);
		JavaRDD<Row>rddJ = df.javaRDD();
		System.out.println(rddJ.toDebugString());
	}

}
