package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class Singler {

	private int reportTypeId;
	private int reportId;
	private int parentId;
	private int stockId;
	private long nbrRecs;
	private double r2;
	
	public int getReportTypeId() {
		return reportTypeId;
	}
	public void setReportTypeId(int reportTypeId) {
		this.reportTypeId = reportTypeId;
	}
	public int getReportId() {
		return reportId;
	}
	public void setReportId(int reportId) {
		this.reportId = reportId;
	}
	public int getParentId() {
		return parentId;
	}
	public void setParentId(int parentId) {
		this.parentId = parentId;
	}
	public int getStockId() {
		return stockId;
	}
	public void setStockId(int stockId) {
		this.stockId = stockId;
	}
	public long getNbrRecs() {
		return nbrRecs;
	}
	public void setNbrRecs(long nbrRecs) {
		this.nbrRecs = nbrRecs;
	}
	public double getR2() {
		return r2;
	}
	public void setR2(double r2) {
		this.r2 = r2;
	}
	
	public int insert() throws BlsException {
		
		int retval = -1;
		Connection conn = Utils.getDbConnection();
		String sql = "insert into singler (report_type_id, report_id, parent_id, stock_id, nbr_recs, r2) values (?,?,?,?,?,?)";
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, 1);
			ps.setInt(2, reportId);
			ps.setInt(3, parentId);
			ps.setInt(4, stockId);
			ps.setLong(5, nbrRecs);
			ps.setDouble(6, r2);
			retval = ps.executeUpdate();
			
		}
		catch (SQLException se) {
			throw new BlsException("Cannot insert singler: " + se.getLocalizedMessage());
		}
		
		return  retval;
	}
	
}
