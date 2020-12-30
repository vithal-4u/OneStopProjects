package com.spark.java.poc.pojo;

import java.io.Serializable;
import java.util.List;

public class RuleBookColumnsPojo implements Serializable{
	public List<RuleBookColumns> RuleBookColumns;

	public List<RuleBookColumns> getRuleBookColumns() {
		return RuleBookColumns;
	}

	public void setRuleBookColumns(List<RuleBookColumns> RuleBookColumns) {
		this.RuleBookColumns = RuleBookColumns;
	}


}
