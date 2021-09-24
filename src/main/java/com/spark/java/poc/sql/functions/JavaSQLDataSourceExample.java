package com.spark.java.poc.sql.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
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
		String path = "D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\";
		SparkSession spark = SparkUtils.createSparkSession("Java Spark SQL data sources example");
		System.setProperty("hadoop.home.dir", "D:\\Unzip_Softwares\\winutils\\bin");
		//runBasicDataSourceExample(spark);
		//runBasicParquetExample(spark,path);
		//runParquetSchemaMergingExample(spark,path);
		runJsonDatasetExample(spark,path);
	}

	private static void runJsonDatasetExample(SparkSession spark, String path) {
		// $example on:json_dataset$
	    // A JSON dataset is pointed to by path.
	    // The path can be either a single text file or a directory storing text files
	    Dataset<Row> people = spark.read().json(path+"people.json");

	    // The inferred schema can be visualized using the printSchema() method
	    people.printSchema();
	    // root
	    //  |-- age: long (nullable = true)
	    //  |-- name: string (nullable = true)

	    // Creates a temporary view using the DataFrame
	    people.createOrReplaceTempView("people");

	    // SQL statements can be run by using the sql methods provided by spark
	    Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
	    namesDF.show();
	    // +------+
	    // |  name|
	    // +------+
	    // |Justin|
	    // +------+

	    // Alternatively, a DataFrame can be created for a JSON dataset represented by
	    // a Dataset<String> storing one JSON object per string.
	    List<String> jsonData = Arrays.asList(
	            "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
	    Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
	    Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);
	    anotherPeople.show();
	    // +---------------+----+
	    // |        address|name|
	    // +---------------+----+
	    // |[Columbus,Ohio]| Yin|
	    // +---------------+----+
	    // $example off:json_dataset$
	}

	private static void runParquetSchemaMergingExample(SparkSession spark, String path) {
		// $example on:schema_merging$
	    List<Square> squares = new ArrayList<>();
	    for (int value = 1; value <= 5; value++) {
	      Square square = new Square();
	      square.setValue(value);
	      square.setSquare(value * value);
	      squares.add(square);
	    }

	    // Create a simple DataFrame, store into a partition directory
	    Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
	    squaresDF.write().parquet(path+"data/test_table/key=1");
	    
	    System.out.println("++++++++++++++ Create Square DataFrame ++++++++++++++");
	    squaresDF.printSchema();
	    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
	    
	    List<Cube> cubes = new ArrayList<>();
	    for (int value = 6; value <= 10; value++) {
	      Cube cube = new Cube();
	      cube.setValue(value);
	      cube.setCube(value * value * value);
	      cubes.add(cube);
	    }

	    // Create another DataFrame in a new partition directory,
	    // adding a new column and dropping an existing column
	    Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
	    cubesDF.write().parquet(path+"data/test_table/key=2");
	    
	    System.out.println("++++++++++++++ Create Cube DataFrame ++++++++++++++");
	    cubesDF.printSchema();
	    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");

	    // Read the partitioned table
	    Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet(path+"data/test_table");
	    
	    System.out.println("++++++++++++++ Merge Schema ++++++++++++++");
	    mergedDF.printSchema();
	    System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");

	    // The final schema consists of all 3 columns in the Parquet files together
	    // with the partitioning column appeared in the partition directory paths
	    // root
	    //  |-- value: int (nullable = true)
	    //  |-- square: int (nullable = true)
	    //  |-- cube: int (nullable = true)
	    //  |-- key: int (nullable = true)
	    // $example off:schema_merging$
	}

	private static void runBasicParquetExample(SparkSession spark, String path) {
		 // $example on:basic_parquet_example$
	    Dataset<Row> peopleDF = spark.read().json("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\people.json");

	    // DataFrames can be saved as Parquet files, maintaining the schema information
	    peopleDF.write().parquet(path+"people.parquet");

	    // Read in the Parquet file created above.
	    // Parquet files are self-describing so the schema is preserved
	    // The result of loading a parquet file is also a DataFrame
	    Dataset<Row> parquetFileDF = spark.read().parquet(path+"people.parquet");

	    // Parquet files can also be used to create a temporary view and then used in SQL statements
	    parquetFileDF.createOrReplaceTempView("parquetFile");
	    Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
	    Dataset<String> namesDS = namesDF.map(
	        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
	        Encoders.STRING());
	    namesDS.show();
	    // +------------+
	    // |       value|
	    // +------------+
	    // |Name: Justin|
	    // +------------+
	    // $example off:basic_parquet_example$
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
