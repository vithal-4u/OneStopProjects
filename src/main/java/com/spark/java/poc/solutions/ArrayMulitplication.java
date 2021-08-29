package com.spark.java.poc.solutions;

import java.sql.Array;

public class ArrayMulitplication {

	public static void main(String[] args) {
		int arr[]= {2,3,4};
		for(int i=0; i<=arr[i];i++){
			int tempIndex = arr[i];
			
			for(int j = arr.length; j<=arr[j-1]; j--) {
				if(arr[j-1] != tempIndex) {
					System.out.println("Array place -- "+arr[j-1]  +"---- Index multiple with --"+tempIndex+ "--Actual Val--"+arr[j-1]*tempIndex);
				}
			}
			
		}
	}

}
