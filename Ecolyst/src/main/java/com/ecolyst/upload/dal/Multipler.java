package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class Multipler {

	static Logger logger = Logger.getLogger("com.blsProcessor");

	private int id;
	private int reportTypeId;
	private int reportId;
	private int stockId;
	private int nbrRecs;
	private String coeffs;
	private float r2;
	private float intercept;
	private List<Predictions> predictions;
	
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
	public int getReportId() {
		return reportId;
	}
	public void setReportId(int reportId) {
		this.reportId = reportId;
	}
	public int getStockId() {
		return stockId;
	}
	public void setStockId(int stockId) {
		this.stockId = stockId;
	}
	public int getNbrRecs() {
		return nbrRecs;
	}
	public void setNbrRecs(int nbrRecs) {
		this.nbrRecs = nbrRecs;
	}
	public String getCoeffs() {
		return coeffs;
	}
	public void setCoeffs(String coeffs) {
		this.coeffs = coeffs;
	}
	
	public float getR2() {
		return r2;
	}
	public void setR2(float r2) {
		this.r2 = r2;
	}
	public float getIntercept() {
		return intercept;
	}
	public void setIntercept(float intercept) {
		this.intercept = intercept;
	}
	public List<Predictions> getPredictions() {
		return predictions;
	}
	public void setPredictions(List<Predictions> predictions) {
		this.predictions = predictions;
	}
	
	public static void clear() throws BlsException {
		String sql = "delete from multipler";
		String autoGen = "alter table multipler auto_increment 1";
		Connection conn = Utils.getDbConnection();
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			ps = conn.prepareStatement(autoGen);
			ps.executeUpdate();
		}
		catch (SQLException se) {
			throw new BlsException ("SQLException deleting multipler", se);
		}
		Predictions.clear();
	}

	
	public int insert() throws BlsException {
		
		int retval = -1;
		
		Connection conn = Utils.getDbConnection();
		String sql = "insert into Multipler (report_type_id, report_id, stock_id, nbr_recs, coeffs, r2, intercept) values (?,?,?,?,?,?,?) on duplicate key update nbr_recs=?,coeffs=?,r2=?,intercept=?";

		try {
			conn.setAutoCommit(false);
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, 1);
			ps.setInt(2, reportId);
			ps.setInt(3, stockId);
			ps.setInt(4, nbrRecs);
			ps.setString(5,  coeffs);
			ps.setFloat(6, Float.isFinite(r2)?r2:0); 
			ps.setFloat(7, Float.isFinite(intercept)?intercept:0);

			ps.setInt(8, nbrRecs);
			ps.setString(9,  coeffs);
			ps.setFloat(10, Float.isFinite(r2)?r2:0); 
			ps.setFloat(11, Float.isFinite(intercept)?intercept:0);

			retval = ps.executeUpdate();
	        try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                logger.debug("Could not insert into multipler, reportId: " + reportId + " : Sched_id: " +stockId );
	            }
	        }
	        Predictions.delete(id);
	        for ( Predictions pred : getPredictions() ) {
	        	pred.setMultipler(id);
		        pred.insert();
	        }
	        conn.commit();
	        conn.setAutoCommit(true);
		}
		
		catch ( SQLException se ) {
			try {
				conn.rollback();
			} catch (SQLException e) {}
			try {
				conn.setAutoCommit(true);
			} catch (SQLException e) {}
			throw new BlsException("Cannot insert a multipler result", se);
		}

		return retval;
	}

}
