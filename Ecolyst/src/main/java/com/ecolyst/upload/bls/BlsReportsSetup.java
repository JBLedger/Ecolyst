package com.ecolyst.upload.bls;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.common.Utils;
import com.ecolyst.upload.dal.BlsReport;
import com.ecolyst.upload.dal.BlsSubReport;

/**
 * This class gets the report and sub report names and codes
 * 
 * It's used to setup the database for running the BLS API
 * 
 * This class is idempotent
 * 
 * @author LedgerZen
 *
 */
public class BlsReportsSetup implements IStep {
	
	//URL to the reports we're interested in
	private static final String TOP_URL = "https://data.bls.gov/cgi-bin/surveymost?bls";

	//A map of report codes to id's from the database
	private Map<String, Integer> reportMap = null;

	/**
	 * Entry point for pipeline	
	 */
	@Override
	public void process() throws BlsException {
		
		String contents = Utils.getUrlContents(TOP_URL);
		
		//regex for finding report codes (group 1) and names (group 2) in contents
		Pattern pcode = Pattern.compile( "DD><INPUT TYPE=checkbox NAME=series_id VALUE=(.*?)>(.*?)<" );
		Matcher mcode = pcode.matcher(contents);
		int mcount = 0;
		while (mcode.find()) {
			
			logger.debug("group 1:" + mcode.group(1).trim());
			logger.debug("group 2:" + mcode.group(2).trim());
			
		    BlsSubReport sub = new BlsSubReport();
		    String rcode = mcode.group(1).trim();
		    if ( !rcode.startsWith("MPU") ) {
			    sub.setReportCode(mcode.group(1).trim());
			    sub.setBlsReportId(getReportIdfromSubReportCode(sub.getReportCode()));
			    sub.setName(mcode.group(2).trim());
			    sub.insert();
			    mcount++;
		    }
		}
		logger.info("number of sub reports: " + mcount);
		contents = null;
		pcode = null;
		mcode = null;
	}
	

	/**
	 * Gets the main report id for the given sub report id
	 * 
	 * @param code - the sub report code
	 * @return the main report id
	 * @throws BlsException if there's issues getting report id's from the database
	 */
	private int getReportIdfromSubReportCode(String code) throws BlsException {
		
		String em = "-LNS-CES-";
		String prodp = "-PRS-";	
		String prodr = "-PRS85006092-";
		String cpi = "-CUUR-CWUR-";
		String ppi = "--WP-";
		String ix  = "-EIU-";
		String comp ="-CIU-";
		String reportCode = null;
		if ( em.indexOf(code.substring(0,3)) > 0 ) {
			reportCode = "em";
		}
		else if ( code.length() > 9 && prodr.indexOf(code.substring(0,10)) > 0 ) {
			reportCode = "prodr";
		}
		else if ( prodp.indexOf(code.substring(0,3)) > 0 ) {
			reportCode = "prodp";
		}
		else if ( cpi.indexOf(code.substring(0,4)) > 0 ) {
			reportCode = "cpi";
		}
		else if ( ppi.indexOf(code.substring(0,2)) > 0 ) {
			reportCode = "ppi";
		}
		else if ( ix.indexOf(code.substring(0,3)) > 0 ) {
			reportCode = "ix";
		}
		else if ( comp.indexOf(code.substring(0,3)) > 0 ) {
			reportCode = "comp";
		}
		return getReportId(reportCode);
	}
	
	/**
	 * Gets the report id from the databases
	 * 
	 * @param code The code to map to the required id
	 * @return - the main report id
	 * @throws BlsException is thrown if there's issues find the report id from the database
	 */
	private int getReportId(String code) throws BlsException {
		
		if ( reportMap == null ) {
			reportMap = BlsReport.getReportCodes();
		}
		return reportMap.get(code);
	}

	@Override
	public void destroy() {
		reportMap = null;
	}
	
	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		BlsReportsSetup br = new BlsReportsSetup();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		try {
			br.process();
		}
		catch (BlsException be) {
			logger.error(be);
		}
	}

}
