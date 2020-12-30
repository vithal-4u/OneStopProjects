package com.spark.java.poc.pojo;

import java.io.Serializable;
import java.util.List;

public class RuleBookColumns implements Serializable{
	public String COLUMN_NAME;
	public List<Rules> RULES;

	public String getColumn_name() {
		return COLUMN_NAME;
	}

	public void setColumn_name(String COLUMN_NAME) {
		this.COLUMN_NAME = COLUMN_NAME;
	}

	public List<Rules> getRules() {
		return RULES;
	}

	public void setRules(List<Rules> RULES) {
		this.RULES = RULES;
	}
}
