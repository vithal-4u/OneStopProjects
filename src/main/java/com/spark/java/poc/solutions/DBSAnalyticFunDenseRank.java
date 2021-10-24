package com.spark.java.poc.solutions;

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

/**
 *  DBS Company ask this question in interview 
 *  Q. Find the 3 max salary of the employee from the dataset 
 * 
 * @author kasho
 *
 */
public class DBSAnalyticFunDenseRank {

	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkUtils.createSparkSession("DBS Analytic Functions Dense Rank");
		List<Row> rows = Arrays.asList(
				   RowFactory.create(1, 10, 1),
				   RowFactory.create(2,10,1),
				   RowFactory.create(3,10,1),
				   RowFactory.create(4,5,2),
				   RowFactory.create(5,2,3),
				   RowFactory.create(6,5,4),
				   RowFactory.create(7,4,2));
		
		StructType schema = DataTypes
	            .createStructType(new StructField[] {DataTypes.createStructField("employeeId", DataTypes.IntegerType, false),DataTypes.createStructField("salary", DataTypes.IntegerType, false),
	                    DataTypes.createStructField("dept id", DataTypes.IntegerType, false)});
		Dataset<Row> datasetRows =spark.createDataFrame(rows, schema);
		datasetRows.show(false);
		
		datasetRows.createTempView("Employee");
		
		spark.sql("select * from (select *,dense_rank() over(order by salary desc)  as salary_num from Employee) a where a.salary_num=3").show();
	}
}
