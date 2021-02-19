package com.spark.java.poc.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import com.spark.java.poc.utils.SparkUtils;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;

public class SingleColumnUDFExmple {

	// Create udf to prefix 00 in single column (UDF1 is used)
	public static UDF1<String, String> formatId = new UDF1<String, String>() {

		// @Override
		public String call(String id) throws Exception {
			System.out.println("Inside UDF --- "+"00" + id);
			return "00" + id;
		}
	};
	@SuppressWarnings({ "serial" })
	public static void main(String[] args) {
		try {
			
			// setup spark configuration and create spark context/spark session
			SparkSession sparkSession = SparkUtils.createSparkSession("Single-column-udf");

			// Load dataset
			Dataset<Row> products = sparkSession.read().format("csv").option("header", "true")
					.load("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\products.csv");

			// display the products before applying UDF
			products.show(false);

			// register udf in your spark session
			sparkSession.udf().register("format_id", SingleColumnUDFExmple.formatId, DataTypes.StringType);

			// use udf to pass id column as input and get the output in new column -
			// formatted_id
			products = products.withColumn("formatted_id", callUDF("format_id", col("id")));

			// display the products after applying UDF
			products.show(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
