package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class Analysis {

	Logger logger = Logger.getLogger("com.blsProcessor");

	private final static String CODE_COUNT_SQL = "select max(total) from (select count(*) as total from eco_reports group by parent_id) as results";
	private static int CODE_COUNT = 0;
	
	/**
	 * ? 1 - parent id
	 * ? 2 - stock id
	 * ? 3 - parent id
	 * ? 4 - count
	 */
	private final static String CONFIDENCE_SQL = 
			"select " + 
			"(" + 
			"select count(*) from quotes q, eco_reports er where q.sched_id in ( " + 
			"select sched_Id from eco_reports where parent_id = ?) " + 
			"and stock_symbol_id = ? " + 
			"and q.sched_id = er.sched_id " + 
			"and parent_id = ? and ((q.delta >=0 and er.delta >=0) or (q.delta<0 and er.delta<0)) " + 
			") " + 
			"/  " + 
			"? "; 

	/**
	 * ? 1 - parent id
	 * ? 2 - stock id
	 * ? 3 - parent id
	 */
	private final static String STRENGTH_SQL = "select avg(q.delta/er.delta) from quotes q, eco_reports er where q.sched_id in ( " + 
			"select sched_Id from eco_reports where parent_id = ?) " + 
			"and stock_symbol_id = ? " + 
			"and q.sched_id = er.sched_id " + 
			"and parent_id = ? and " +
			"((q.delta >0 and er.delta >0) or (q.delta<0 and er.delta<0)) ";

	private int id;
	private int stock;
	private int parent;
	private float confidence;
	private float strength;
	
	public int getCodeCount() throws BlsException {
		
		if ( CODE_COUNT == 0 ) {

			Connection con = Utils.getDbConnection();
			try {
				Statement pstate = con.createStatement();
				ResultSet rs = pstate.executeQuery(CODE_COUNT_SQL);
				if ( rs.next() ) {
					CODE_COUNT = rs.getInt(1);
				}

			} catch (SQLException e) {
				throw new BlsException ("Error inserting Code Count: " + e.getLocalizedMessage());
			}
		}
		
		return CODE_COUNT;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getStock() {
		return stock;
	}
	public void setStock(int stock) {
		this.stock = stock;
	}
	public int getParent() {
		return parent;
	}
	public void setParent(int parent) {
		this.parent = parent;
	}
	public float getConfidence() {
		return confidence;
	}
	public void setConfidence(float confidence) {
		this.confidence = confidence;
	}
	public float getStrength() {
		return strength;
	}
	public void setStrength(float strength) {
		this.strength = strength;
	}
	
	public void calculateConfidence() throws BlsException {

		Connection con = Utils.getDbConnection();
		try {
			PreparedStatement pstate = con.prepareStatement(CONFIDENCE_SQL, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, parent);
			pstate.setInt(2, stock);
			pstate.setInt(3, parent);
			pstate.setInt(4, getCodeCount());
			ResultSet rs = pstate.executeQuery();
			if ( rs.next() ) {
				this.confidence = rs.getFloat(1);
			}

		} catch (SQLException e) {
			throw new BlsException ("Error inserting Analysis row: " + e.getLocalizedMessage());
		}
		
	}
	
	public void calculateStrength() throws BlsException {
		
		Connection con = Utils.getDbConnection();
		try {
			PreparedStatement pstate = con.prepareStatement(STRENGTH_SQL, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, parent);
			pstate.setInt(2, stock);
			pstate.setInt(3, parent);
			ResultSet rs = pstate.executeQuery();
			if ( rs.next() ) {
				this.strength = rs.getFloat(1);
			}

		} catch (SQLException e) {
			throw new BlsException ("Error inserting Analysis row: " + e.getLocalizedMessage());
		}
		
	}
	
	
	
	
	public int insert() throws BlsException {
		
		String sql = "insert into Analysis (stock_id, parent_id, confidence, strength) values (?,?,?,?)";
		Connection con = Utils.getDbConnection();
		int id = -1;
		try {
			PreparedStatement pstate = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, stock);
			pstate.setInt(2, parent);
			pstate.setFloat(3, confidence);
			pstate.setFloat(4, strength);
			pstate.executeUpdate();

	        try (ResultSet generatedKeys = pstate.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                throw new BlsException("Creating Analysis row failed, no ID obtained.");
	            }
	        }
		} catch (SQLException e) {
			throw new BlsException ("Error inserting Analysis row: " + e.getLocalizedMessage());
		}
		return id;

	}
	

}
