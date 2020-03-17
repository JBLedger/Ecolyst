package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.IStep;
import com.ecolyst.upload.common.Utils;

/**
 * Class to add any tombstone data to the database.
 * At this point the only tombstone data consists of the BLS report types.
 * 
 * The 'process' method can be run multiple times without corrupting the database.
 * 
 * @author LedgerZen
 *
 */
public class TombstoneData implements IStep {

	/**
	 * Creates the necessary report type rows.
	 * The 'ignore' in the insert statements ensures that this method can be 
	 * run multiple times without corrupting the databases with multiple
	 * rows of the same data.
	 */
	private String[] inserts;
	{
		inserts = new String[9];
		inserts[0] = "insert ignore into markets (id, name) values (1, 'nyse')";
		inserts[1] = "insert ignore into report_types (id, name) values (1, 'bls')";
		inserts[2] = "insert ignore into bls_reports (id, code) values (1,'em')";
		inserts[3] = "insert ignore into bls_reports (id, code) values (2,'prodp')";
		inserts[4] = "insert ignore into bls_reports (id, code) values (3,'prodr')";
		inserts[5] = "insert ignore into bls_reports (id, code) values (4,'cpi')";
		inserts[6] = "insert ignore into bls_reports (id, code) values (5,'ppi')";
		inserts[7] = "insert ignore into bls_reports (id, code) values (6,'ix')";
		inserts[8] = "insert ignore into bls_reports (id, code) values (7,'comp')";
	}
	
	@Override
	public void process() throws BlsException {
		
		Connection con = Utils.getDbConnection();
		for (String sql : inserts ) {
			try {
				PreparedStatement pstate = con.prepareStatement(sql);
				logger.debug("Inserting: " + sql);
				pstate.executeUpdate();
				
			} catch (SQLException e) {
				throw new BlsException("Cannot insert tombstone data", e);
			}
		}
	}


	@Override
	public void destroy() {
		inserts = null;
	}
	
	
	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */		
	public static void main(String[] args) {
		
		TombstoneData td = new TombstoneData();
		Logger.getRootLogger().setLevel(Level.DEBUG);
		try {
			td.process();
		} catch (BlsException e) {
			logger.error(e);
		}
		
	}

}
