package com.spark.java.poc.transformation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.spark.java.poc.pojo.Employee;
import com.spark.java.poc.utils.SparkUtils;

public class FlatMapToDoubleTransformation {

	public static void main(String[] args) {
		JavaRDD<String> strRDD =SparkUtils.readTextFile("FlatMapToDoubleTransformation", "employees.csv",1);
		String header = strRDD.first();
		JavaRDD<String> withOutHeader = strRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String row) throws Exception {
				return !row.equals(header);
			}
		});
		JavaDoubleRDD emplNoDoubleRDD = withOutHeader.flatMapToDouble(new DoubleFlatMapFunction<String>() {
			
			@Override
			public Iterator<Double> call(String row) throws Exception {
				String arry[] = row.split(",");
				Double emplNoDoble= new Double(arry[0]);
				return Arrays.asList(emplNoDoble).iterator();			
			}
		});
		
		List<Double> data = emplNoDoubleRDD.collect();
		for(Double emplNo: data) {
			System.out.println(emplNo);
		}
	}

}
