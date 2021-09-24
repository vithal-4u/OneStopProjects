package com.spark.java.poc.sql.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import com.spark.java.poc.utils.SparkUtils;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaSparkSQLExample {

	// $example on:create_ds$
	  public static class Person implements Serializable {
	    private String name;
	    private int age;

	    public String getName() {
	      return name;
	    }

	    public void setName(String name) {
	      this.name = name;
	    }

	    public int getAge() {
	      return age;
	    }

	    public void setAge(int age) {
	      this.age = age;
	    }
	  }
	  
	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkUtils.createSparkSession("JavaSparkSQLExample");
		//runBasicDataFrameExample(spark);
		//runDatasetCreationExample(spark);
		runProgrammaticSchemaExample(spark);
	}

	private static void runProgrammaticSchemaExample(SparkSession spark) {
		 // Create an RDD
	    JavaRDD<String> peopleRDD = spark.sparkContext()
	      .textFile("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\people.txt", 1)
	      .toJavaRDD();
	    
	 // The schema is encoded in a string
	    String schemaString = "name age mobile";
	    
	 // Generate the schema based on the string of schema
	    List<StructField> fields = new ArrayList();
	    for (String fieldName : schemaString.split(" ")) {
	      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
	      fields.add(field);
	    }
	    StructType schema = DataTypes.createStructType(fields);
	 // Convert records of the RDD (people) to Rows
	    JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
	      String[] attributes = record.split(",");
	      return RowFactory.create(attributes[0], attributes[1].trim());
	    });
	    
	 // Apply the schema to the RDD
	    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

	    // Creates a temporary view using the DataFrame
	    peopleDataFrame.createOrReplaceTempView("people");

	    // SQL can be run over a temporary view created using DataFrames
	    Dataset<Row> results = spark.sql("SELECT * FROM people");
	    
	    results.show();
	    // The results of SQL queries are DataFrames and support all the normal RDD operations
	    // The columns of a row in the result can be accessed by field index or by field name
	    Dataset<String> namesDS = results.map(
	        (MapFunction<Row, String>) row -> "Name: " + row.getString(0) + " Age: " + row.getString(1),
	        Encoders.STRING());
	    namesDS.show();
	}

	private static void runDatasetCreationExample(SparkSession spark) {
		// $example on:create_ds$
	    // Create an instance of a Bean class
	    Person person = new Person();
	    person.setName("Andy");
	    person.setAge(32);
	    
	    
	    Encoder<Person> personEncoder = Encoders.bean(Person.class);
	    spark.createDataset(
	    	      Collections.singletonList(person),
	    	      personEncoder
	    	    ).show();
	    
	 // Encoders for most common types are provided in class Encoders
	    Encoder<Integer> integerEncoder = Encoders.INT();
	    Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
	    Dataset<Integer> transformedDS = primitiveDS.map(
	        (MapFunction<Integer, Integer>) value -> value + 1,
	        integerEncoder);
	    transformedDS.collect(); 
	    
	    String path = "D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\people.json";
	    Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
	    peopleDS.show();
	}

	private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
		Dataset<Row> df = spark.read().json("D:\\Study_Document\\Git_Hub\\OneStopProjects\\resources\\people.json");
		df.show();
		
		df.printSchema();
	    // root
	    // |-- age: long (nullable = true)
	    // |-- name: string (nullable = true)
		
		df.select("name").show();
		df.select(col("name"), col("age").plus(1)).show();
		df.filter(col("age").gt(21)).show();
		
		
		df.createOrReplaceTempView("people");

	    Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
	    sqlDF.show();
		
	 // $example on:global_temp_view$
	    // Register the DataFrame as a global temporary view
	    df.createGlobalTempView("people");

	    // Global temporary view is tied to a system preserved database `global_temp`
	    spark.sql("SELECT * FROM global_temp.people").show();
	    
	}

}
