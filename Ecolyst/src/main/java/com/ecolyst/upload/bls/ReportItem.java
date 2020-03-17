package com.ecolyst.upload.bls;

import org.apache.log4j.Logger;

public class ReportItem {
	
	Logger logger = Logger.getLogger("com.blsProcessor");
	
	
	private String year;
	private String month;
	private String value;
	
	
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public String getMonth() {
		return month;
	}
	public void setMonth(String month) {
		this.month = month;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	
	

}
