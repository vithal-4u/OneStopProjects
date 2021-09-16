package com.spark.java.poc.pojo;

import java.io.Serializable;

public class Employee implements Serializable{
	private String emp_no;
	private String first_name;
	private String last_name;
	private String hire_date;
	private String birth_date;
	private String gender;
	

	public Employee(String emp_no, String birth_date, String first_name,String last_name, String gender, String hire_date) {
		super();
		this.emp_no = emp_no;
		this.first_name = first_name;
		this.last_name = last_name;
		this.hire_date = hire_date;
		this.birth_date = birth_date;
		this.gender = gender;
	}
	public String getFirst_name() {
		return first_name;
	}

	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}

	public String getLast_name() {
		return last_name;
	}

	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}

	public String getHire_date() {
		return hire_date;
	}

	public void setHire_date(String hire_date) {
		this.hire_date = hire_date;
	}

	public String getBirth_date() {
		return birth_date;
	}

	public void setBirth_date(String birth_date) {
		this.birth_date = birth_date;
	}
	public String getEmp_no() {
		return emp_no;
	}
	public void setEmp_no(String emp_no) {
		this.emp_no = emp_no;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	@Override
	public String toString() {
		return "Employee - [Name -- "+ getEmp_no() + " . First_name -- "+ getFirst_name()+ " , Last_name -- "+ getLast_name()+ " , Gender -- "+getGender()+" , Birth_date -- "+
					getBirth_date()+" , Hire_date -- "+getHire_date()+"]";
	}

	
}
