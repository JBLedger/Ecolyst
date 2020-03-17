package com.ecolyst.analysis;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.IStep;
import com.ecolyst.upload.bls.StockSymbol;
import com.ecolyst.upload.dal.BlsSubReport;

/**
 * This step updates the delta value (column) in the:
 *  - quote table, where delta is the % difference in price from open and close for the day 
 *  - eco_report table, where the delta is the % difference between current report and 
 *    last report (previous month)
 *    
 * The 'analyse()' method is under construction
 * 
 * @author LedgerZen
 *
 */
public class ReportStockCamparison implements IStep {
	
	/**
	 * Process the calculation of the deltas by table, quotes and eco_reports
	 */
	public void process() throws BlsException {

		StockSymbol.populateAvgDeltas();
		BlsSubReport.populateAvgDeltas();

	}

	/**
	 * Under construction
	 */
//	private void analyse() throws BlsException {
//		
//		List<StockSymbol> stocks = StockSymbol.getAllUnprocessedNyseStocksBySymbol();
//		List<BlsSubReport> reports = BlsSubReport.getAnalysableReportCodes();
//		int nbrOfRows = stocks.size() * reports.size();
//		logger.debug("Generating " + nbrOfRows + " analysis reports");
//		
//		for ( StockSymbol stock : stocks ) {
//			//check to make sure a quote exists 
//			for ( BlsSubReport report : reports ) {
//				Analysis an = new Analysis();
//				an.setStock(stock.getId());
//				an.setParent(report.getId());
//				an.calculateConfidence();
//				an.calculateStrength();
//				an.insert();
//			}
//		}
//	}

	@Override
	public void destroy() {
		//nothing to clean up
	}
	
	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */		
	public static void main (String[] args) {
		
		ReportStockCamparison rsc = new ReportStockCamparison();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		try {
			rsc.process();
		}
		catch (BlsException be) {
			logger.error("Could not perform stock comparison", be);
		}
	}
}
