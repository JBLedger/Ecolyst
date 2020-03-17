package com.ecolyst.analysis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.IStep;
import com.ecolyst.upload.common.Utils;
import com.ecolyst.upload.dal.EcoReport;
import com.ecolyst.upload.dal.Quotes;

/**
 * Pivot table structure
---------------------
unique key is stock_id and sched_id
sub_report_? values will be repeated for every stock_id
stock_id  sched_id   sub_report_id_1   sub_report_id_2   sub_report_id_?  stock_price
--------  --------   ----------------  ---------------   ---------------  -----------
 1   		1  			.5     				.8     			.1    			 1.4
 1   		2  			.3     				.7     			.2    			 .6
 
For each major report (cpi, ppi, imex, job, jol)
  create a table with columns based on sub reports
end for

For each stock
   get all sched_ids
   for each sched_id
      get the reports
      populate above table
   end for
end for  
    
for each distinct stock sid in pivot table
   results = select * from pivot where stock_id = sid
   run regression on results
end  for

 * @author LedgerZen
 *
 */
public class PivotData implements IStep {
	
	/**
	 * This process takes call the sub-reports that make up a BLS report,
	 * and pivots the data such that unique rows are by stock id and sched id, 
	 * and columns are the sub-reports.
	 * The stock delta is included as a column for ease of analysis
	 * 
	 * Note that the generated tables are dropped and recreated every time this
	 * method is run.
	 * 
	 * This takes a long time - 2 to 3 hours or so.  I'm assuming this can be improved,
	 * perhaps using a different language
	 * 
	 */
	public void process() throws BlsException {
		
		PivotTables.generatePivotTables();
		
		//get all quotes in maps by sched id, stock id
		logger.info("Loading all clean quotes by sched id, stock id");
		Map<Integer, Map<Integer, Quotes>> quotes = Quotes.getCleanQuotesMapBySchedIdStockId();
		
		//get all eco_reports by schedId, stockId
		logger.info("Loading all economic reports by sched id, stock id");
		Map<Integer, Map<Integer, Map<Integer, EcoReport>>> allEcoReportsMap = EcoReport.getReportsBySchedIdParentId();
		//1 represents stocks from nyse, which are the only stocks in use at this time
		Map<Integer, Map<Integer, EcoReport>> ecoReportsMap = allEcoReportsMap.get(1);
		
		//get sub reports as an element of a map representing the 5 main reports
		Map<String, List<Integer>> subReports = PivotTables.getReportsAndSubReports();
		 
		//for all schedules
		try {
			processSchedIds(subReports, quotes, ecoReportsMap);
		}
		catch (SQLException se) {
			throw new BlsException("SQL Exception caught pivoting tables: " + se.getLocalizedMessage());
		}
		
		quotes = null;
		allEcoReportsMap = null;
		ecoReportsMap = null;
		subReports = null;
	}
	
	private void processSchedIds(
			Map<String, List<Integer>> subReports, 
			Map<Integer, Map<Integer, Quotes>> quotes, 
			Map<Integer, Map<Integer, EcoReport>> ecoReportsMap) 
	throws BlsException, SQLException {
		
		Connection con = Utils.getDbConnection();
		for ( String reportKey : subReports.keySet() ) {
			
			String tableName = "gnr_" + reportKey;
			List<Integer> subReportIds = subReports.get(reportKey);
			List<Integer> schedIds = PivotTables.getUniqueSchedIds(reportKey);
			logger.debug("Number of schedIds for " + reportKey +": " + schedIds.size());
			logger.debug(schedIds);
			
			String insertSql = getGenTableInsertSql(tableName, subReportIds);

			PreparedStatement pstate = con.prepareStatement(insertSql);
	
			//for each sched id
			for ( Integer schedId : schedIds ) {
				//for each stock
				Map<Integer, Quotes> schedQuotes = quotes.get(schedId);
				for ( Integer quoteId : schedQuotes.keySet() ) {
					Quotes quote = schedQuotes.get(quoteId);
					pstate.setInt(1, quote.getStockSymbolId());
					pstate.setInt(2, quote.getSchedId());
					pstate.setFloat(3, quote.getDelta());
					int indCount = 4;
					for (Integer subReportId : subReportIds ) {
						if ( ecoReportsMap.get(schedId) != null && 
								ecoReportsMap.get(schedId).get(subReportId) != null ) {
							pstate.setFloat(indCount++, ecoReportsMap.get(schedId).get(subReportId).getDelta());
							logger.debug("Sched id: " + schedId + "   subReportId: " + subReportId);
						}
						else {
							logger.debug("NULL Sched id: " + schedId + "   subReportId: " + subReportId);
							pstate.setFloat(indCount++, java.sql.Types.NULL);
						}
					}
					int ex = pstate.executeUpdate();
					logger.debug("insert into " + tableName + ": " + ex);
				}
			}
		}
	}

		
	/**
	 * Build the insert string for the generated pivot table
	 * @param ids
	 * @return
	 */
	private String getGenTableInsertSql(String tableName, List<Integer> ids) {
		StringBuilder sb = new StringBuilder("insert into ");
		sb.append(tableName);
		sb.append(" (stock_id, sched_id, stock_delta ");
		for ( Integer id : ids ) {
			sb.append(", sr_" );
			sb.append(id);
		}
		sb.append(") values (?, ?, ?");
		for ( int x = 0; x < ids.size(); x++ ) {
			sb.append(", ?");
		}
		sb.append(")");
		
		return sb.toString();

	}



	@Override
	public void destroy() {
		//nothing to clean
	}

	
	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */		
	public static void main(String[] args) {
		PivotData pd = new PivotData();
		Logger.getRootLogger().setLevel(Level.DEBUG);
		try {
			pd.process();
		} catch (BlsException e) {
			logger.error("Cannot pivot data", e);
		}
	}
}
