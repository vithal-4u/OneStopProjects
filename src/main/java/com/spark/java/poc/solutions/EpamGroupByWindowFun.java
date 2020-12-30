package com.spark.java.poc.solutions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import com.spark.java.poc.utils.SparkUtils;

public class EpamGroupByWindowFun {

	public static void main(String[] args) {
		SparkSession sparkSess = SparkUtils.createSparkSession("EpamGroupByWindowFun");
		Dataset<Row> empFileData = sparkSess.read().option("header", "true").csv("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\EmpRandom.csv");
		//empFileData.show();
		Dataset<Row> groupAggData =empFileData.groupBy("Dep","Location").agg(sum("Salary"));
		groupAggData.show();
		//		
		//		+---+--------+-----------+
		//		|Dep|Location|sum(Salary)|
		//		+---+--------+-----------+
		//		|  A|     PUN|   143000.0|
		//		| IT|     PUN|   170000.0|
		//		| IT|     Ban|   124000.0|
		//		| IT|     Hyd|    60000.0|
		//		|  A|     Hyd|    75000.0|
		//		|  A|     Ban|    60000.0|
		//		+---+--------+------------
		//		
		
		//WindowSpec window= Window.partitionBy("Location");
		
		//empFileData.withColumn("maxSal", functions.max(col("Salary"))).where(col("sum(Salary)") == col("maxSal"));
	}

}
