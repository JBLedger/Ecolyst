package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class LinearRegression {
	
	static Logger logger = Logger.getLogger("com.blsProcessor");

	public static void createPivotTable() throws BlsException {

		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			int x = stmt.executeUpdate(dropPivotTableString());
			logger.debug("Pivot table dropped: " + x);
		}
		catch (SQLException se) {
			logger.error("Could not drop pivot table", se);
		}

		try {
			Statement stmt = con.createStatement();
			int x = stmt.executeUpdate(createPivotTableString());
			logger.debug("Pivot table created: " + x);
		}
		catch (SQLException se) {
			logger.error("Could not create pivot table", se);
		}
	}
	
	private static String dropPivotTableString() {

		return ("drop table if exists cash.lin_reg; ");

	}
	
	private static String createPivotTableString() throws BlsException {
		
		//get all rows from bls_sub_reports
		
		StringBuilder sb = new StringBuilder("create table cash.lin_reg ( \r\n");
		sb.append("  stock_id INT(11) NOT NULL,\r\n");
		sb.append("  sched_id INT(11) NOT NULL,\r\n");
		sb.append("  stock_delta FLOAT NOT NULL,\r\n");
		List<BlsSubReport> subs = BlsSubReport.getAllReportCodes();
		for ( BlsSubReport bsr : subs ) {
			sb.append("  sr_");
			sb.append(bsr.getId());
			sb.append(" FLOAT, \r\n");
		}
		sb.append(" UNIQUE INDEX pivot_stock_parent_uniq(stock_id, sched_id) );");
		return sb.toString();
	}
	
	
}
