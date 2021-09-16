package com.spark.java.poc.transformation;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import com.amazonaws.services.dynamodbv2.xspec.S;
import com.spark.java.poc.utils.SparkUtils;

/**
 * This class used Filter Transformation to skip header like of CSV file.
 * 
 * @author kasho
 *
 */
public class JavaFilterTrasf {

	public static void main(String[] args) {
		JavaRDD<String> strRDD =SparkUtils.readTextFile("MapTransformation", "employees.csv",1);
		String header = strRDD.first();
		JavaRDD<String> withOutHeader = strRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String row) throws Exception {
				return !row.equals(header);
			}
		});		
		
		List<String> data = withOutHeader.collect();
		for(String str: data) {
			System.out.println(str);
		}
	}

}
