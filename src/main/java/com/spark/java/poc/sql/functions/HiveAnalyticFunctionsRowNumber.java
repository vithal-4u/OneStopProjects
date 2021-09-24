package com.spark.java.poc.sql.functions;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

public class HiveAnalyticFunctionsRowNumber {

	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkUtils.createSparkSession("Hive Analytic Functions Row Number");
		Dataset<Row> peopleDFCsv = spark.read().format("csv").option("sep", ",").option("inferSchema", "true")
				.option("header", "true").load("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\students.csv");
		
		peopleDFCsv.createTempView("Student");
		
		/**
		 * We will show how Student table looks 
		 */
		Dataset<Row> results = spark.sql("Select * from Student");
		results.show(false);

		
		/**
		 * Row_number Implementation:
		 * 				Hive Row_Number or SQL Row Number helps to serve 2 main purposes:
		 * 					i. Generate a unique sequence number for all the rows or a group of rows.
		 * 					ii. Helps eliminate duplicate rows.
		 * 		
		 * 		#### Generating a Sequence Number
		 * 
		 */
		
		spark.sql("select * , row_number() over(order by Roll_No) as row_num from Student").show();
		
		
		/**
		 * 		#### Eliminating Duplicates using row_number
		 * 		The point to keep in mind here is, row_number will return a 1 for all unique rows. Any number other than 1 is a duplicate row on the specified window.
		 * 
		 * 
		 */
		
		spark.sql("select * ,row_number() over(partition by student_name, roll_no, dept order by roll_no)  as row_num from Student").show();
		
		// Now filter only unique records:
		spark.sql("select * from (select * ,row_number() over(partition by student_name, roll_no, dept order by roll_no)  as row_num from Student) a where a.row_num = 1").show();
		
		
//		Dataset<String> rows= SparkUtils.readTextFileDataset("Hive Analytic Functions Example","students.csv");
//		rows.createTempView("Student");

	}

}
