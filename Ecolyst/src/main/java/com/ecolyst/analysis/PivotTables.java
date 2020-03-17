package com.ecolyst.analysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class PivotTables {
	
	static {
		Logger.getLogger("org").setLevel(Level.INFO);
		Logger.getLogger("akka").setLevel(Level.INFO);
	}
	static Logger logger = Logger.getLogger("com.blsProcessor");
	
	
	public static Map<String, List<Integer>> subReports = null;
	
	public static void generatePivotTables() throws BlsException {
		
		dropGeneratedTables();

		Connection con = Utils.getDbConnection();
		
		//for each report code (map key), get sub report id's (list)
		Map<String, List<Integer>> subReports = getReportsAndSubReports();
		
		//generate pivot tables
		try {
			Statement stmt = con.createStatement();
			for ( String key : subReports.keySet() ) {
				String genTableSql = generateTableSql(key, subReports.get(key));
				logger.debug(genTableSql);
				int x = stmt.executeUpdate(genTableSql);
				logger.debug("Pivot table created for " + key +": " + x);
			}
		}
		catch (SQLException se) {
			throw new BlsException(se.getLocalizedMessage());
		}
	}
	
	/**
	 * Gets all unique schedule ids for all stocks and given report
	 * 
	 * @param report
	 * @return
	 * @throws BlsException
	 */
	public static List<Integer> getUniqueSchedIds(String report) throws BlsException {
		
		List<Integer> schedIds = new ArrayList<Integer>();
		String sql = "select id from sched_dates where id in "
				+ "(select sched_id from clean_quotes) and id in "
				+ "(select sched_id from eco_reports where parent_id in "
				+ "(select id from bls_sub_reports where bls_report_id in " 
				+ "(select id from bls_reports where code = ?))) "
				+ "order by id"; 
		Connection con = Utils.getDbConnection();
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, report);
			ResultSet rs = ps.executeQuery();
			while (rs.next() ) {
				schedIds.add(rs.getInt(1));
			}
		}
		catch (SQLException se) {
			throw new BlsException(se.getLocalizedMessage());
		}
		return schedIds;
	}
	
	private static String generateTableSql(String key, List<Integer> ids) throws BlsException {
		
		StringBuilder sql = new StringBuilder("create table gnr_");
		sql.append(key);
		sql.append(" (\r\n");
		sql.append("  stock_id INT(11) NOT NULL,\r\n");
		sql.append("  sched_id INT(11) NOT NULL,\r\n");
		sql.append("  stock_delta FLOAT NOT NULL,\r\n");
		for ( Integer id : ids ) {
			sql.append("  sr_");
			sql.append(id);
			sql.append(" FLOAT, \r\n");
		}
		sql.append(" UNIQUE INDEX pivot_stock_parent_uniq_");
		sql.append(key);
		sql.append(" (stock_id, sched_id) )");
		return sql.toString();
	}
	
	
	/**
	 * Returns all bls sub report id'*s in sections (lists) by report code (map key).
	 * 
	 * @return
	 * @throws BlsException
	 */
	public static Map<String, List<Integer>> getReportsAndSubReports() throws BlsException {
		
		if ( subReports == null ) {
			List<String> reports = getReportCodes();
			subReports = new HashMap<String, List<Integer>>();
			String sql = "select bsr.id from bls_sub_reports bsr, bls_reports br where bsr.bls_report_Id = br.id and br.code = ?";
			Connection con = Utils.getDbConnection();
			try {
				PreparedStatement ps = con.prepareStatement(sql);
				for ( String report : reports ) {
					ps.setString(1, report);
					ResultSet rs = ps.executeQuery();
					List<Integer> currentReport = new ArrayList<Integer>();
					subReports.put(report, currentReport);
					while ( rs.next() ) {
						currentReport.add(rs.getInt(1));
					}
				}
			}
			catch (SQLException se) {
				throw new BlsException (se.getLocalizedMessage());
			}
		}

		return subReports;
	}
	
	/**
	 * Returns a list of major bls report names 
	 * @return
	 * @throws BlsException
	 */
	private static List<String> getReportCodes() throws BlsException {
		String reportCodesSql = "select code from bls_reports";
		Connection con = Utils.getDbConnection();
		List<String> reportCodes = new ArrayList<String>();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(reportCodesSql);
			while (rs.next() ) {
				reportCodes.add( rs.getString(1) );
			}
		}
		catch (SQLException se) {
			throw new BlsException(se.getLocalizedMessage());
		}
		return reportCodes; 
	}
	
	private static void dropGeneratedTables() throws BlsException {
		
		List<String> drops = dropPivotTablesString();
		Connection con = Utils.getDbConnection();
		
		try {
			for ( String sql : drops ) {
				PreparedStatement ps = con.prepareStatement(sql);
				int x = ps.executeUpdate();
				logger.debug(sql + ": " + x);
			}
		}
		catch (SQLException e) {
			throw new BlsException(e.getLocalizedMessage());
		}
	}
	
	private static List<String> dropPivotTablesString() throws BlsException {

		
		String tableNamesSql = "SELECT table_name FROM information_schema.tables where table_name like 'gnr_%'";
		Connection con = Utils.getDbConnection();
		List<String> tables = new ArrayList<String>();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(tableNamesSql);
			while (rs.next() ) {
				tables.add("drop table " + rs.getString(1) );
			}
		}
		catch (SQLException e) {
			throw new BlsException("Issues getting generated table names: " + e.getLocalizedMessage());
		}
		return tables;
	}
	
	
	public static List<String> getPivotTables() throws BlsException {

		
		String tableNamesSql = "SELECT table_name FROM information_schema.tables where table_name like 'gnr_%'";
		Connection con = Utils.getDbConnection();
		List<String> tables = new ArrayList<String>();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(tableNamesSql);
			while (rs.next() ) {
				tables.add(rs.getString(1) );
			}
		}
		catch (SQLException e) {
			throw new BlsException("Issues getting generated table names: " + e.getLocalizedMessage());
		}
		return tables;
	}

	
}
