package com.spark.java.poc.sql.functions;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.spark.java.poc.utils.SparkUtils;

public class JavaSQLDataSourceExample {
	// $example on:schema_merging$
	public static class Square implements Serializable {
		private int value;
		private int square;

		// Getters and setters...
		// $example off:schema_merging$
		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getSquare() {
			return square;
		}

		public void setSquare(int square) {
			this.square = square;
		}
		// $example on:schema_merging$
	}
	// $example off:schema_merging$

	// $example on:schema_merging$
	public static class Cube implements Serializable {
		private int value;
		private int cube;

		// Getters and setters...
		// $example off:schema_merging$
		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		public int getCube() {
			return cube;
		}

		public void setCube(int cube) {
			this.cube = cube;
		}
		// $example on:schema_merging$
	}

	public static void main(String[] args) {
		SparkSession spark = SparkUtils.createSparkSession("Java Spark SQL data sources example");
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		runBasicDataSourceExample(spark);
	}

	private static void runBasicDataSourceExample(SparkSession spark) {
		Dataset<Row> usersDF = spark.read()
				.load("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\users.parquet");
		usersDF.show();
		usersDF.select("name", "favorite_color").write()
				.save("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\namesAndFavColors.parquet");

		Dataset<Row> peopleDF = spark.read().format("json")
				.load("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\people.json");
		peopleDF.select("name", "age").write().format("parquet")
				.save("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\namesAndAges.parquet");

		Dataset<Row> peopleDFCsv = spark.read().format("csv").option("sep", ";").option("inferSchema", "true")
				.option("header", "true").load("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\people.csv");

		peopleDFCsv.write().format("orc").option("orc.bloom.filter.columns", "favorite_color")
				.option("orc.dictionary.key.threshold", "1.0")
				.save("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\users_with_options.orc");

		Dataset<Row> sqlDF = spark
				.sql("SELECT * FROM parquet.`D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\users.parquet`");
		sqlDF.show();

		peopleDF.write().bucketBy(42, "name").sortBy("age")
				.saveAsTable("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\people_bucketed");

		usersDF.write().partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet");

		peopleDF.write().partitionBy("favorite_color").bucketBy(42, "name").saveAsTable("people_partitioned_bucketed");

		spark.sql("DROP TABLE IF EXISTS people_bucketed");
		spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed");
	}

}
