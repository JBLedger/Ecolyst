package com.ecolyst.upload.bls;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.dal.BlsReport;
import com.ecolyst.upload.dal.SchedCode;

/**
 * This step determines the BLS report dates.
 * 
 *  There were issues reading the contents of the web site programmatically, 
 *  so the pages were saved to text and this function reads the text files.
 *  Currently there is data back to 2015.  If more is needed, in a browser
 *  navigate to the BLS site for the year required, view source, and copy/paste
 *  into a text document.
 * 
 * @author LedgerZen
 *
 */
public class BlsSchedSetup implements IStep {
	
	private Map<String, Integer> reportIds = null;
	
	private SimpleDateFormat SDF = new SimpleDateFormat("EEEE, MMMM dd, yyyy");
	
	private  HashMap<String, String> schedCodes;
	 {
		schedCodes = new HashMap<String, String>();
		schedCodes.put("Consumer Price Index", "cpi");
		schedCodes.put("Producer Price Index", "ppi");
		schedCodes.put("Productivity and Costs (P)", "prodp");
		schedCodes.put("Productivity and Costs (R)", "prodr");
		schedCodes.put("Employment Situation", "em");
		schedCodes.put("U.S. Import and Export Price Indexes", "ix");
		schedCodes.put("Employment Cost Index","comp");
	}
	
	private static final String REGEX = "<td class=\"date-cell\"><p>(.*?)</p></td>\r\n" + 
			"<td class=\"time-cell\"><p>(.*?)</p></td>\r\n" + 
			"<td class=\"desc-cell\"><p><strong>(.*?)</strong>";

	/**
	 * Processes the BLS calendar files by year
	 * 
	 */
	public void process() throws BlsException {
		
		String contents = "";
		for ( int year = 2015; year < 2021; year++ ) {
			contents = loadSchedule("y" + String.valueOf(year) + ".txt");
			logger.debug("year: " + year);
			processContents(contents);
		}
	}

	/**
	 * Uses regex to find the date and description, and saves to database. 
	 * 
	 * @param contents
	 * @throws BlsException
	 */
	private void processContents(String contents) throws BlsException {
		
		Pattern pattern = Pattern.compile( REGEX );
		Matcher m = pattern.matcher(contents);
		String desc = "";
		Date date = null;
		while (m.find()) {
			try {
				date = SDF.parse(m.group(1));
			}
			catch (ParseException pe) {
				throw new BlsException("Issues parsing date from BLS: " + m.group(1), pe);
			}
			desc = m.group(3);
		    String code = schedCodes.get(desc);
			if ( code != null ) {
				saveSched(date, code);
			}
		}
	}

	/**
	 * Creates a Schedule Code db object, and persists it
	 * 
	 * @param date from the bls schedule calendar
	 * @param code from the bls schedule calendar
	 * @throws BlsException if there's issues saving to the database
	 */
	private void saveSched(Date date, String code) throws BlsException {
		SchedCode scodes = new SchedCode();
		scodes.setReport_type_id(1);
		scodes.setSched_date(date);
		scodes.setReport_id(getReportId(code));
		scodes.insert();
	}
	
	/**
	 * Returns the contents for the given filename
	 * 
	 * @param filename - name of the file to read
	 * @return the contents of the file
	 * @throws BlsException if there's issues reading the file from the file system (IO exceptions)
	 */
	private String loadSchedule(String filename) throws BlsException {
		
		byte[] fileBytes = null;
		try {
			InputStream in = BlsSchedSetup.class.getClassLoader().getResourceAsStream(filename);
			fileBytes = new byte[in.available()];
			in.read(fileBytes);
		}
		catch (IOException ioe) {
			throw new BlsException("Cannot load file: " + filename + " : " + ioe.getLocalizedMessage());
		}
		return new String(fileBytes);
	}
	
	/**
	 * Loads the report id's from the database and returns the single requested id
	 * 
	 * @param code - the report code for the requested id 
	 * @return - the id for the given code
	 * @throws BlsException - if there's database issues
	 */
	private int getReportId(String code) throws BlsException {
		
		if (reportIds == null ) {
			reportIds = BlsReport.getReportCodes();
		}
		return reportIds.get(code);
	}


	@Override
	public void destroy() {
		reportIds = null;
		schedCodes = null;
		SDF = null;
	}
	
	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */	
	public static void main (String[] args) {
		
		BlsSchedSetup sched = new BlsSchedSetup();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		try {
			sched.process();
		} catch (BlsException e) {
			logger.error(e);
		}
		
	}
}
