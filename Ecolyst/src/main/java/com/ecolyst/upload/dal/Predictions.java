package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class Predictions {

	private int id;
	private int multipler;
	private float label;
	private float pred;
	private String features = "";
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getMultipler() {
		return multipler;
	}
	public void setMultipler(int multipler) {
		this.multipler = multipler;
	}
	public float getLabel() {
		return label;
	}
	public void setLabel(float label) {
		this.label = label;
	}
	public float getPred() {
		return pred;
	}
	public void setPred(float pred) {
		this.pred = pred;
	}
	public String getFeatures() {
		return features;
	}
	public void setFeatures(String features) {
		this.features = features;
	}

	public int insert() throws BlsException {
		
		int retval = -1;
		
		Connection conn = Utils.getDbConnection();
		String sql = "insert into multipler_predictions (multipler_id, label, prediction, features) values (?,?,?,?) ";

		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, multipler);
			ps.setFloat(2, label);
			ps.setFloat(3, pred);
			ps.setString(4, features);
			retval = ps.executeUpdate();
		}
		catch ( SQLException se ) {
			throw new BlsException("Cannot insert a multipler result: " + se.getLocalizedMessage());
		}

		return retval;
	}
	
	public static void delete (int multipler) throws BlsException {
		String sql = "delete from multipler_predictions where multipler_id = ?";
		Connection conn = Utils.getDbConnection();
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setInt(1, multipler);
			ps.executeUpdate();
		}
		catch (SQLException se) {
			throw new BlsException ("SQLException deleting prediction for mulitpler: " + multipler, se);
		}
	}

	public static void clear () throws BlsException {
		String sql = "delete from multipler_predictions";
		String autoGen = "alter table multipler_predictions auto_increment 1";
		Connection conn = Utils.getDbConnection();
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			ps = conn.prepareStatement(autoGen);
			ps.executeUpdate();
		}
		catch (SQLException se) {
			throw new BlsException ("SQLException deleting prediction", se);
		}
	}

	
}
