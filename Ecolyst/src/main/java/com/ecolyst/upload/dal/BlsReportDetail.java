package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class BlsReportDetail {
	
	Logger logger = Logger.getLogger("com.blsProcessor");

	private int id;
	private int blsReportId;
	private String reportName;
	private String url;
	
	
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


	public String getReportName() {
		return reportName;
	}


	public void setReportName(String reportName) {
		this.reportName = reportName;
	}


	public String getUrl() {
		return url;
	}


	public void setUrl(String url) {
		this.url = url;
	}


	public int save() throws BlsException {
		
		String sql = "insert into bls_report_details ( bls_report_id, report_name, url) select ?, ?, ? from dual where not exists (select 1 from bls_report_details where url = ?)";
		Connection con = Utils.getDbConnection();
		int id = -1;
		try {
			PreparedStatement pstate = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, blsReportId);
			pstate.setString(2, reportName);
			pstate.setString(3, url.replace("?", "."));
			pstate.setString(4, url.replace("?", "."));
			pstate.executeUpdate();

	        try (ResultSet generatedKeys = pstate.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                logger.debug("Skipping duplicate entry of Report Detail.");
	    			PreparedStatement psel = con.prepareStatement("select id from bls_report_details where url = ?");
	    			psel.setString(1, url.replace("?", "."));
	    			ResultSet rs = psel.executeQuery();
	    			if ( rs.next() ) {
	    				id = rs.getInt(1);
	    			}
	    			else {
	    				throw new BlsException("Duplicate Bls Report Detail causing issues");
	    			}
	            }
	        }
		} catch (SQLException e) {
			throw new BlsException(e.getLocalizedMessage());
		}
		return id;
		
	}

}
