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

public class EcoReport {
	
	static Logger logger = Logger.getLogger("com.blsProcessor");

	private int id;
	private int reportTypeId;
	private int parentId;
	private int schedId;
	private float value;
	private float delta;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getReportTypeId() {
		return reportTypeId;
	}
	public void setReportTypeId(int reportTypeId) {
		this.reportTypeId = reportTypeId;
	}
	public int getParentId() {
		return parentId;
	}
	public void setParentId(int parentId) {
		this.parentId = parentId;
	}
	public int getSchedId() {
		return schedId;
	}
	public void setSchedId(int schedId) {
		this.schedId = schedId;
	}
	public float getValue() {
		return value;
	}
	public void setValue(float value) {
		this.value = value;
	}
	public float getDelta() {
		return delta;
	}
	public void setDelta(float delta) {
		this.delta = delta;
	}
	
	public EcoReport getPreviousEcoReport() throws BlsException {
		
		EcoReport er = null;
		String sql = "select id, report_type_id, parent_id, sched_id, value, delta from eco_reports " + 
				" where report_type_id = " + reportTypeId + " and parent_id = " + parentId+ " and sched_id = (" + 
				"       select max(sched_id) from eco_reports " + 
				"		where report_type_id = " + reportTypeId + " and parent_id = " + parentId + " and sched_id < " + schedId + ")"; 
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if ( rs.next() ) {
				er = new EcoReport();
				er.setId(rs.getInt(1));
				er.setReportTypeId(rs.getInt(2));
				er.setParentId(rs.getInt(3));
				er.setSchedId(rs.getInt(4));
				er.setValue(rs.getFloat(5));
				er.setDelta(rs.getFloat(6));
			}
			
		} catch (SQLException e) {
			throw new BlsException("Cannot update delta: " + e.getLocalizedMessage());
		}
		
		return er;
	}
	
	public static List<EcoReport> getAllReports() throws BlsException {
		
		List<EcoReport> reports = new ArrayList<EcoReport>();
		String sql = "select id, report_type_id, parent_id, sched_id, value, delta from eco_reports";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next() ) {
				EcoReport eco = new EcoReport();
				eco.setId(rs.getInt(1));
				eco.setReportTypeId(rs.getInt(2));
				eco.setParentId(rs.getInt(3));
				eco.setSchedId(rs.getInt(4));
				eco.setValue(rs.getFloat(5));
				eco.setDelta(rs.getFloat(6));
				reports.add(eco);
			}
		} catch (SQLException e) {
			throw new BlsException ("Cannot get all Eco Reports: ", e);
		}
		return reports;
	}
	
	public static List<EcoReport> getAllReportsOrderByTypeSched() throws BlsException {
		
		List<EcoReport> reports = new ArrayList<EcoReport>();
		String sql = "select id, report_type_id, parent_id, sched_id, value, delta from eco_reports order by report_type_id, sched_id, parent_id";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next() ) {
				EcoReport eco = new EcoReport();
				eco.setId(rs.getInt(1));
				eco.setReportTypeId(rs.getInt(2));
				eco.setParentId(rs.getInt(3));
				eco.setSchedId(rs.getInt(4));
				eco.setValue(rs.getFloat(5));
				eco.setDelta(rs.getFloat(6));
				reports.add(eco);
			}
		} catch (SQLException e) {
			throw new BlsException ("Cannot get all Eco Reports: ", e);
		}
		return reports;
	}
	
	public static List<EcoReport> getAllReportsOrderByTypeParent(int parent_id) throws BlsException {
		
		List<EcoReport> reports = new ArrayList<EcoReport>();
		String sql = "select id, report_type_id, parent_id, sched_id, value, delta from eco_reports order by report_type_id, parent_id, sched_id where parent_id = ?";
		Connection con = Utils.getDbConnection();
		try {
			PreparedStatement stmt = con.prepareStatement(sql);
			stmt.setInt(1, parent_id);
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next() ) {
				EcoReport eco = new EcoReport();
				eco.setId(rs.getInt(1));
				eco.setReportTypeId(rs.getInt(2));
				eco.setParentId(rs.getInt(3));
				eco.setSchedId(rs.getInt(4));
				eco.setValue(rs.getFloat(5));
				eco.setDelta(rs.getFloat(6));
				reports.add(eco);
			}
		} catch (SQLException e) {
			throw new BlsException ("Cannot get all Eco Reports by type parent: ", e);
		}
		return reports;
	}
	
	public void updateDelta() throws BlsException {
		
		String sql = "update eco_reports set delta = ? where id = ?";
		Connection con = Utils.getDbConnection();
		try {
			PreparedStatement pstate = con.prepareStatement(sql);
			pstate.setFloat(1, delta);
			pstate.setInt(2, id);
			pstate.executeUpdate();
			
		} catch (SQLException e) {
			throw new BlsException("Cannot update delta: " + e.getLocalizedMessage());
		}
		
	}
	
	/**
	 * the 3 maps of the return type represent the 3 keys in the eco_reports table:
	 *  1. report_type (always 1 for now, representing bls reports)
	 *  2. sched_id (schedule id)
	 *  3. parent_id (the bls_sub_report_id)
	 *  
	 * @return
	 * @throws BlsException
	 */
	public static Map<Integer, Map<Integer, Map<Integer, EcoReport>>> getReportsBySchedIdParentId () throws BlsException {
		
		Map<Integer, Map<Integer, Map<Integer, EcoReport>>> maps = new HashMap<Integer, Map<Integer, Map<Integer, EcoReport>>>();
		Map<Integer,Map<Integer,EcoReport>> schedMaps = new HashMap<Integer, Map<Integer, EcoReport>>();
		maps.put(1, schedMaps);
		
		List<EcoReport> allReports = getAllReportsOrderByTypeSched();
		logger.debug("allReports size: " + allReports.size());
		
		for ( EcoReport report : allReports ) {
			Map<Integer, EcoReport> parentMaps = schedMaps.get(report.getSchedId());
			if ( parentMaps == null ) {
				parentMaps = new HashMap<Integer, EcoReport>();
				schedMaps.put(report.getSchedId(), parentMaps);
			}
			parentMaps.put(report.getParentId(), report);
		}
		return maps;
	}
	
	/**
	 * the 3 maps of the return type represent the 3 keys in the eco_reports table:
	 *  1. report_type (always 1 for now, representing bls reports)
	 *  2. sched_id (schedule id)
	 *  3. parent_id (the bls_sub_report_id)
	 *  
	 * @return
	 * @throws BlsException
	 */ 
	public static Map<Integer, Map<Integer, EcoReport>> getReportsByParentIdSchedId () throws BlsException {
		
		Map<Integer,Map<Integer,EcoReport>> parentMaps = new HashMap<Integer, Map<Integer, EcoReport>>();
		
		List<EcoReport> allReports = getAllReportsOrderByTypeSched();
		for ( EcoReport report : allReports ) {
			Map<Integer, EcoReport> schedMaps = parentMaps.get(report.getSchedId());
			if ( schedMaps == null ) {
				schedMaps = new HashMap<Integer, EcoReport>();
				parentMaps.put(report.getSchedId(), schedMaps);
			}
			schedMaps.put(report.getParentId(), report);
		}
		return parentMaps;
	}
	
	public int insert() throws BlsException {
		
		String sql = "insert into eco_reports (report_type_id, parent_id, sched_id, value) select ?,?,?,? where not exists (select '1' from eco_reports where parent_id = ? and sched_id = ?)";
		Connection con = Utils.getDbConnection();
		int id = 0;
		try {
			PreparedStatement pstate = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, reportTypeId);
			pstate.setInt(2, parentId);
			pstate.setInt(3, schedId);
			pstate.setFloat(4, value);
			pstate.setInt(5, parentId);
			pstate.setInt(6, schedId);
			pstate.executeUpdate();
			
	        try (ResultSet generatedKeys = pstate.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                logger.debug("Could not insert into eco_reports, duplicate detected: parent_id: " + parentId + " : Sched_id: " + schedId );
	            }
	        }
		} catch (SQLException e) {
			throw new BlsException("Cannot insert eco_reports: " + e.getLocalizedMessage());
		}
		return id;
	}
}
