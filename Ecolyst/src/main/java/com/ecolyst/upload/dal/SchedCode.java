package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class SchedCode {

	Logger logger = Logger.getLogger("com.blsProcessor");

	private int id;
	private Date sched_date;
	private int report_type_id;
	private int report_id;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public Date getSched_date() {
		return sched_date;
	}
	public void setSched_date(Date sched_date) {
		this.sched_date = sched_date;
	}
	public int getReport_type_id() {
		return report_type_id;
	}
	public void setReport_type_id(int report_type_id) {
		this.report_type_id = report_type_id;
	}
	public int getReport_id() {
		return report_id;
	}
	public void setReport_id(int report_id) {
		this.report_id = report_id;
	}
	
	public void insert() throws BlsException {
		
		String sql = "insert into sched_dates (sched_date, report_type_id, report_id) select ?,?,? from dual where not exists (select 1 from sched_dates where sched_date = ?)";
		Connection con = Utils.getDbConnection();
		try {
			PreparedStatement pstate = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			java.sql.Date sqldate = new java.sql.Date(sched_date.getTime());
			pstate.setDate(1, sqldate);
			pstate.setInt(2, report_type_id);
			pstate.setInt(3, report_id);
			pstate.setDate(4, sqldate);
			pstate.executeUpdate();
			
	        try (ResultSet generatedKeys = pstate.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                logger.debug("Skipping duplicate schedule date: " + sqldate);
	            }
	        }
		} catch (SQLException e) {
			throw new BlsException("Cannot insert sched_code: " + e.getLocalizedMessage());
		}
	}
	
	public static int findId (String year, String month, int report_id) throws BlsException {
		
		int id = 0;
		int y = Integer.parseInt(year);
		int m = Integer.parseInt(month);
		String mo = "0" + String.valueOf(m);
		mo = mo.substring ( mo.length() - 2 );
		String ye = String.valueOf(y);
		String sql = "select id from sched_dates where YEAR(sched_date) = '" + ye +
				"' and MONTH(sched_date) = '" + mo + "' and report_id = " + report_id;
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next() ) {
				id = rs.getInt(1);
			}
		} catch (SQLException e) {
			throw new BlsException ("Unable to perform findId queury: "  + e.getLocalizedMessage());
		}
		return id;
	}

	
	public static List<SchedCode> findAll () throws BlsException {

		List<SchedCode> schedCodes = new ArrayList<SchedCode>();
		String sql = "select id, sched_date, report_type_id, report_id from sched_dates";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next() ) {
				SchedCode sc = new SchedCode();
				sc.setId(rs.getInt(1));
				sc.setSched_date(rs.getDate(2));
				sc.setReport_type_id(rs.getInt(3));
				schedCodes.add(sc);
			}
		} catch (SQLException e) {
			throw new BlsException ("Unable to perform findId queury: "  + e.getLocalizedMessage());
		}
		return schedCodes;
	}
	
	public static int findPreviousSchedId(int sid) {
		
		
		
		return 0;
	}

}
