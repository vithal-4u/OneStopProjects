package com.spark.java.poc.solutions;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.java.poc.utils.SparkUtils;

import scala.Tuple2;

/**
 * 		I have a DF of employee with a city column. In this DF, there are multiple employees belonging to a city. 
 * 			I want to assign unique to each diff city in this data frame. How to do this?
 * 
 * @author kasho
 *
 */
public class AmazonAnalyticFunctionDenseRank {

	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkUtils.createSparkSession("Hive Analytic Functions Row Number");
		
		List<Tuple2<String, String>> mixedTypes = Arrays.asList(
                new Tuple2<>("raj", "mumbai"),
                new Tuple2<>("ramesh", "mumbai"),
                new Tuple2<>("sateesh", "hyderabad"),
                new Tuple2<>("gopal", "pune"),
                new Tuple2<>("sri", "hyderabad"),
                new Tuple2<>("suresh", "pune"));
		
		
		List<Row> rows = Arrays.asList(
				   RowFactory.create("raj", "mumbai"),
				   RowFactory.create("ramesh", "mumbai"),
				   RowFactory.create("sateesh", "hyderabad"),
				   RowFactory.create("gopal", "pune"),
				   RowFactory.create("sri", "hyderabad"),
				   RowFactory.create("suresh", "pune"));
		StructType schema = DataTypes
	            .createStructType(new StructField[] { DataTypes.createStructField("emp", DataTypes.StringType, false),
	                    DataTypes.createStructField("city", DataTypes.StringType, false) });
		Dataset<Row> datasetRows =spark.createDataFrame(rows, schema);
		datasetRows.show(false);
		
		datasetRows.createTempView("Employee");
		
		/**
		 * We will show how Student table looks 
		 */
		Dataset<Row> results = spark.sql("Select * from Employee");
		results.show(false);
		
		spark.sql("select * , row_number() over(order by city) as row_num from Employee").show();
		
		spark.sql("select * ,dense_rank() over(order by city)  as row_num from Employee").show();
	}

}
