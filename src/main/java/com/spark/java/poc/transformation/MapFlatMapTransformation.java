package com.spark.java.poc.transformation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.spark.java.poc.pojo.Employee;
import com.spark.java.poc.utils.SparkUtils;

public class MapFlatMapTransformation {

	public static void main(String[] args) {
		JavaRDD<String> strRDD =SparkUtils.readTextFile("MapFlatMapTransformation", "employees.csv",1);
		String header = strRDD.first();
		JavaRDD<String> withOutHeader = strRDD.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String row) throws Exception {
				return !row.equals(header);
			}
		});
		
		JavaRDD<Employee> emplRDD = withOutHeader.map(new Function<String, Employee>(){

			@Override
			public Employee call(String row) throws Exception {
				String arry[] = row.split(",");
				if(arry[4].equals("M")) {
					arry[4] = "Male";
				} else if(arry[4].equals("F")) {
					arry[4] = "Fe-Male";
				}
				return new Employee(arry[0],arry[1],arry[2],arry[3],arry[4],arry[5]);
			}
			
		});
		
		JavaRDD<Employee> emplFlatMapRDD = withOutHeader.flatMap(new FlatMapFunction<String, Employee>() {

			@Override
			public Iterator call(String row) throws Exception {
				String arry[] = row.split(",");
				if(arry[4].equals("M")) {
					arry[4] = "Male";
				} else if(arry[4].equals("F")) {
					arry[4] = "Fe-Male";
				}
				Employee empl = new Employee(arry[0],arry[1],arry[2],arry[3],arry[4],arry[5]);
				
				return Arrays.asList(empl).iterator();
			}
		});
		
		
		List<Employee> data = emplFlatMapRDD.collect();
		for(Employee empl: data) {
			System.out.println(empl);
		}
	}

}
