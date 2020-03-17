package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class BlsReport {

	Logger logger = Logger.getLogger("com.blsProcessor");
	
	private int id;
	private String code;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}

	public static Map<String, Integer> getReportCodes() throws BlsException {
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String sql = "select id, code from bls_reports";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next() ) {
				map.put(rs.getString(2), rs.getInt(1));
			}
		} catch (SQLException e) {
			throw new BlsException("Unable to get report codes: " + e.getLocalizedMessage());
		}
		return map;
	}

}
