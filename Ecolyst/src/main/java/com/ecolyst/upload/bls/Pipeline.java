package com.ecolyst.upload.bls;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.analysis.PivotData;
import com.ecolyst.analysis.ReportStockCamparison;
import com.ecolyst.upload.dal.TombstoneData;
import com.ecolyst.upload.stocks.CleanQuotes;
import com.ecolyst.upload.stocks.RawStockSymbolsSetup;
import com.ecolyst.upload.stocks.StocksTiingoProcessor;

/**
 * This class represents the pipeline to get, process and store 
 * the data needed to analyze BLS data.
 * 
 * This process also performs a simple linear regression on the data,
 * but at this point the results are not stored in the database, just
 * haven't got there yet.
 * 
 * - TombstoneData.java - just setup (tombstone) data for db
 * - BlsReportsSetup.java - gets the reports (names, codes, etc) setup in db
 * - BlsSchedSetup.java - gets report schedules setup in db
 * - EcoReportData.java - gets report data using bls api
 * - RawStockSymbolsSetup.java - gets stock symbols setup in database
 * - StocksTiingoProcessor.java - gets open/close stock prices for each report date from Tiingo.  You'll need you're own key for this ($10/month)
 * - ReportStockComparison.java - calculates daily quote deltas and economic report deltas between reports
 * - CleanQuotes.java - data cleansing exercise
 * - Pivots.java - pivots rows/columns of data so that it can be analyzed by Spark
 * - MultipleRegression.java - perform a linear regression using Spark
 * 
 */
public class Pipeline {
	
	private Logger logger = Logger.getLogger("com.blsProcessor");
	
	private void processPipeline () throws BlsException {

		List<IStep> steps = new ArrayList<IStep>();
		steps.add(new TombstoneData());
		steps.add(new BlsReportsSetup());
		steps.add(new BlsSchedSetup());
		steps.add(new EcoReportData());
		steps.add(new RawStockSymbolsSetup());
		steps.add(new StocksTiingoProcessor());
		steps.add(new ReportStockCamparison());
		steps.add(new CleanQuotes());
		steps.add(new PivotData());

		for (IStep step : steps ) {
			logger.info("Pipeline: Processing step " + step.getClass().getCanonicalName());
			step.process();
			step.destroy();
			step = null;
		}
	}

	/**
	 * Entry point - takes no arguments
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		Pipeline pipeline = new Pipeline();
		
		Logger.getRootLogger().setLevel(Level.INFO);
		pipeline.logger.info("Starting to process pipeline");
		
		try {
			pipeline.processPipeline();
		}
		catch (BlsException be) {
			pipeline.logger.error(be);
		}
		
	}
	


}
