package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class BlsSubReport {
	
	Logger logger = Logger.getLogger("com.blsProcessor");

	private int id;
	private int blsReportId;
	private String name;
	private String reportCode;
	private BlsReportDetail parent;
	
	private static List<BlsSubReport> blsSubReportsList;
	private static List<BlsSubReport> analysableReportsList;
	private static Map<String, Integer> reportCodeMap = null;
	private static Map<String, Integer> map = null;
	
	public List<BlsSubReport> getBlsSubReportsList() throws BlsException {
		if ( blsSubReportsList == null ) {
			blsSubReportsList = getAllReportCodes();
		}
		return blsSubReportsList;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getBlsReportId() {
		return blsReportId;
	}
	public void setBlsReportId(int blsReportId) {
		this.blsReportId = blsReportId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getReportCode() {
		return reportCode;
	}
	public void setReportCode(String reportCode) {
		this.reportCode = reportCode;
	}
	public BlsReportDetail getParent() {
		return parent;
	}
	public void setParent(BlsReportDetail parent) {
		this.parent = parent;
	}
	
	public static String[] getStringSeries() throws BlsException {
		
		List<String> reports = new ArrayList<String>();
		
		String[] sa = getAllReportCodesAsAnArray();
		for ( String s : sa ) {
			StringBuffer sb = new StringBuffer();
			for ( int x = 0; x < 25; x++ ) {
				sb.append("\"");
				sb.append(s);
				sb.append("\",");
			}
			sb.deleteCharAt(sb.length()-1);
			reports.add(sb.toString());
		}
		return reports.toArray(new String[0]);
	}

	public static Map<String, Integer> getAllCodesAsReportCodeMap() throws BlsException {
		
		if ( reportCodeMap == null ) {
			reportCodeMap = new HashMap<String, Integer>();
			List<BlsSubReport> sa = getAllReportCodes();
			for ( BlsSubReport bsr : sa ) {
				reportCodeMap.put(bsr.getReportCode(), bsr.getBlsReportId());
			}
		}
		return reportCodeMap;
	}

	public static Map<String, Integer> getAllCodesAsMap() throws BlsException {
		
		if ( map == null ) {
			map = new HashMap<String, Integer>();
			List<BlsSubReport> sa = getAllReportCodes();
			for ( BlsSubReport bsr : sa ) {
				map.put(bsr.getReportCode(), bsr.getId());
			}
		}
		return map;
	}

	public static String[] getAllReportCodesAsAnArray() throws BlsException {

		if ( blsSubReportsList == null ) blsSubReportsList = getAllReportCodes();
		String[] codes = new String[blsSubReportsList.size()];
		int ind = 0;
		for ( BlsSubReport report : blsSubReportsList ) {
			codes[ind++] = report.getReportCode();
		}
		return codes;
	}
	
	public static List<BlsSubReport> getAnalysableReportCodes() throws BlsException {
				
		if ( analysableReportsList == null ) {
			String sql = "select parent_id from eco_reports " + 
					"group by parent_Id having count(*) = ( " + 
					"select max(total) from (select count(*) as total from eco_reports group by parent_id) as results)";
			analysableReportsList = new ArrayList<BlsSubReport>();
			Connection con = Utils.getDbConnection();
			try {
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next() ) {
					BlsSubReport bsr = new BlsSubReport();
					bsr.setId(rs.getInt(1));
					bsr.setBlsReportId(rs.getInt(5));
					bsr.setName(rs.getString(3));
					bsr.setReportCode(rs.getString(4));
					analysableReportsList.add(bsr);
				}
			} catch (SQLException e) {
				throw new BlsException ("Unable to get a connection: " + e.getLocalizedMessage());
			}
		}
		return analysableReportsList;
	}
	
	public static List<BlsSubReport> getAllReportCodes() throws BlsException {
		
		String sql = "select bsr.id, bsr.name, bsr.report_code, br.id from bls_sub_reports bsr, bls_reports br where bsr.bls_report_id = br.id order by bsr.id";
		if ( blsSubReportsList == null ) {
			blsSubReportsList = new ArrayList<BlsSubReport>();
			Connection con = Utils.getDbConnection();
			try {
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next() ) {
					BlsSubReport bsr = new BlsSubReport();
					bsr.setId(rs.getInt(1));
					bsr.setName(rs.getString(2));
					bsr.setReportCode(rs.getString(3));
					bsr.setBlsReportId(rs.getInt(4));
					blsSubReportsList.add(bsr);
				}
			} catch (SQLException e) {
				throw new BlsException ("Unable to get a connection: " + e.getLocalizedMessage());
			}
		}
		return blsSubReportsList;
	}
	
	
	/**
	 * Inserts a bls_sub_reports row ONLY if it doesn't already exist.
	 * 
	 * @return The generated primary key (id) for the row inserted, or the id of the row if the insert was a duplicate
	 * @throws BlsException is thrown to wrap an SQL exception, typically if it cannot connect to the database
	 */
	public int insert() throws BlsException {
		
		String sql = "insert into bls_sub_reports ( bls_report_id, name, report_code) select ?,?,? from dual where not exists (select 1 from bls_sub_reports where report_code = ?)";
		Connection con = Utils.getDbConnection();
		int id = -1;
		try {
			PreparedStatement pstate = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, blsReportId);
			pstate.setString(2, name);
			pstate.setString(3, reportCode);
			pstate.setString(4, reportCode);
			pstate.executeUpdate();
			
			
	        try (ResultSet generatedKeys = pstate.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                logger.debug("Skipping duplicate entry of Sub Report, report Code: " + reportCode);
	    			PreparedStatement psel = con.prepareStatement("select id from bls_sub_reports where report_code = ?");
	    			psel.setString(1, reportCode);
	    			ResultSet rs = psel.executeQuery();
	    			if ( rs.next() ) {
	    				id = rs.getInt(1);
	    			}
	    			else {
	    				throw new BlsException("Duplicate Bls Sub Report causing issues");
	    			}
	            }
	        }
		} catch (SQLException e) {
			throw new BlsException("Issues inserting bls sub reports", e);
		}
		return id;
		
	}
	
	public static void populateAvgDeltas() throws BlsException {
		
		String selectSql = "select parent_id, round(avg(delta),3), round(avg(abs(delta)),3) from eco_reports group by parent_id";
		String updateSql = "update bls_sub_reports set delta_avg = ?, delta_avg_abs = ? where id = ?";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(selectSql);
			PreparedStatement pstate = con.prepareStatement(updateSql);
			while (rs.next() ) {
				pstate.setFloat(1, rs.getFloat(2));
				pstate.setFloat(2, rs.getFloat(3));
				pstate.setInt(3, rs.getInt(1));
				pstate.executeUpdate();
			}
		} catch (SQLException e) {
			throw new BlsException("Issues inserting average deltas", e);
		}
	}


}
