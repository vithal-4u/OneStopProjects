package com.spark.java.poc.sql.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;
import static org.apache.spark.sql.functions.col;

public class JavaSparkSQLDataFrameExample {

	public static void main(String[] args) {	
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		String path = "D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\";
		SparkSession spark = SparkUtils.createSparkSession("Java Spark SQL data sources example");
		 Dataset<Row> peopleDf = spark.read().option("header", true).csv(path+"employees.csv");
		 peopleDf.show(10);
		 
		 //peopleDf.select("emp_no","first_name","hire_date").show(10);
		 peopleDf.select(col("emp_no"),col("first_name").alias("fname"),col("hire_date")).show(10);
		 
		 peopleDf.select(peopleDf.colRegex("*")).show(10);		
	}

}
