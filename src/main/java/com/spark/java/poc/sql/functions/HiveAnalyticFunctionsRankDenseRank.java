package com.spark.java.poc.sql.functions;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.java.poc.utils.SparkUtils;

public class HiveAnalyticFunctionsRankDenseRank {

	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkUtils.createSparkSession("Hive Analytic Functions Rank and Dense Rank");
		
		List<Row> rows = Arrays.asList(
				   RowFactory.create("David",1, "CS", 55, 98),
				   RowFactory.create("Goliath",2, "EE", 32, 94),
				   RowFactory.create("Raven",3, "CS", 40, 83),
				   RowFactory.create("Logan",4, "CS", 70, 99),
				   RowFactory.create("Ravi",5, "EE", 82, 83),
				   RowFactory.create("Amy",7, "EC", 63, 98),
				   RowFactory.create("Johnson",8, "EC", 25, 83),
				   RowFactory.create("Shivani",12, "EE", 67, 80));
		StructType schema = DataTypes
	            .createStructType(new StructField[] { DataTypes.createStructField("Student_Name", DataTypes.StringType, false),DataTypes.createStructField("Roll_No", DataTypes.IntegerType, false),
	                    DataTypes.createStructField("Dept", DataTypes.StringType, false) ,DataTypes.createStructField("Due_Amount", DataTypes.IntegerType, false),
	                    DataTypes.createStructField("Marks", DataTypes.IntegerType, false)});
		
		Dataset<Row> datasetRows =spark.createDataFrame(rows, schema);
		datasetRows.show(false);
		
		datasetRows.createTempView("Student");
		
		//Rank and Dense Rank
		spark.sql("select * , rank() over(order by marks desc) as Rank, dense_rank() over(order by marks desc) as Dense_Rank from Student").show();
		
		//Generate the rank and dense_rank of each student within their respective departments.
		spark.sql("select *, rank() over(partition by dept order by marks desc) as Rank, dense_rank() over(partition by dept  order by marks desc) as Dense_Rank from Student").show();
	}

}
