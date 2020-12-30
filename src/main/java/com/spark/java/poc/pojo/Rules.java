package com.spark.java.poc.pojo;

import java.io.Serializable;

public class Rules implements Serializable{
	public String RULE_NAME;
	public String ATR_MIN;
	public String ATR_MAX;
	public String ATR_TYPE;
	public String ATR_VAL;
	public String getRULE_NAME() {
		return RULE_NAME;
	}
	public void setRULE_NAME(String rULE_NAME) {
		RULE_NAME = rULE_NAME;
	}
	public String getATR_MIN() {
		return ATR_MIN;
	}
	public void setATR_MIN(String aTR_MIN) {
		ATR_MIN = aTR_MIN;
	}
	public String getATR_MAX() {
		return ATR_MAX;
	}
	public void setATR_MAX(String aTR_MAX) {
		ATR_MAX = aTR_MAX;
	}
	public String getATR_TYPE() {
		return ATR_TYPE;
	}
	public void setATR_TYPE(String aTR_TYPE) {
		ATR_TYPE = aTR_TYPE;
	}
	public String getATR_VAL() {
		return ATR_VAL;
	}
	public void setATR_VAL(String aTR_VAL) {
		ATR_VAL = aTR_VAL;
	}

}
