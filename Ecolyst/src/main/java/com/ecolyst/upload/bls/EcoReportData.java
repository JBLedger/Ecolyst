package com.ecolyst.upload.bls;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.common.Constants;
import com.ecolyst.upload.dal.BlsSubReport;
import com.ecolyst.upload.dal.EcoReport;
import com.ecolyst.upload.dal.SchedCode;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * This class gets raw report data using the BLS API
 */
public class EcoReportData implements IStep {
	
	/**
	 * Gets all reports codes as an array from the databases, and processes them
	 * one at a time by getting data from the BLS site.
	 */
	public void process() throws BlsException {
		EcoReportData labor = new EcoReportData();
		
		String[] reports = BlsSubReport.getStringSeries();
		JsonElement je = null;
		for ( String report : reports ) {
			je = labor.getData(report);
			if ( je != null ) labor.processJsonElement(je);
		}
		labor.updateDeltas();
	}
	
	/**
	 * Gets the raw report data in json format for the given series of BLS report codes
	 * 
	 * @param seriesArray - a list of bls report codes
	 * @return the JsonElement with all results from the bls API
	 */
	public JsonElement getData(String series) throws BlsException {
		
		HttpPost httpPost = new HttpPost("https://api.bls.gov/publicAPI/v2/timeseries/data/");
		StringEntity input = null;
		
		try {
			String schedDates = "\"startyear\":\"2015\",\"endyear\":\"2020\"";
			String entity = "{\"seriesid\":[" + series + "], \"registrationKey\":\"" + Constants.BLS_KEY +  "\"," +schedDates + "}";
			logger.debug(entity);
			
			input = new StringEntity(entity);
		} catch (UnsupportedEncodingException e) {
			throw new BlsException("Cannot parse together report codes: " + e.getLocalizedMessage());
		}

		input.setContentType("application/json");
		httpPost.setEntity(input);
        HttpClientBuilder client = HttpClientBuilder.create();
        
        InputStream is = null;
        try {
            HttpResponse response = client.build().execute(httpPost);
			is = response.getEntity().getContent();
		} catch (UnsupportedOperationException | IOException e) {
			throw new BlsException("Cannot get data from BLS web service: " + e.getLocalizedMessage());
		}
        
        return getJsonElement(is);
	}
	
	
	private JsonElement getJsonElement(InputStream is) throws BlsException {
        BufferedReader in = new BufferedReader( new InputStreamReader(is) );
        String current;
        String result = null;
        try {
			while((current = in.readLine()) != null) {
			   result += current;
			}
		} catch (IOException e) {
			throw new BlsException("Cannot get data from web service: " + e.getLocalizedMessage());
		}
        
        JsonParser parser = new JsonParser();
        logger.debug(result);
        JsonElement je = parser.parse(result.substring(4));
        return je;
        
	}
	
	/**
	 * Processes the web results and stores in database 
	 * 
	 * @param je The json object to be parsed
	 * @throws BlsException  Can be thrown for json parsing issues or database issues
	 */
	private void processJsonElement(JsonElement je) throws BlsException {

		JsonObject jo = je.getAsJsonObject();
		JsonObject joResults = jo.getAsJsonObject("Results");
		JsonArray jeSeries = joResults.getAsJsonArray("series");
		
		if ( jeSeries != null ) {
			for ( JsonElement jSet : jeSeries ) {
				String BLSName = jSet.getAsJsonObject().get("seriesID").getAsString();
				int reportId = getReportId(BLSName);
				JsonArray jsonData = jSet.getAsJsonObject().getAsJsonArray("data");
				
				for ( JsonElement jsonEl : jsonData ) {
					JsonObject jsonElObj = jsonEl.getAsJsonObject();
					EcoReport er = new EcoReport();
					er.setReportTypeId(Constants.BLS_REPORT_TYPE);
					er.setValue(Float.parseFloat(jsonElObj.get("value").getAsString()));
					int parentId = getParentId(BLSName);
					er.setParentId(parentId); 
					
					String year = jsonElObj.get("year").getAsString(); 
					String month = jsonElObj.get("period").getAsString().substring(1);
					
					String per = jsonElObj.get("period").getAsString().substring(0,1);
					if ( per.equals("Q") ) {
						if ( reportId == 2 ) {
							if ( month.equals("01") ) {
								month = "05";
							}
							else if ( month.equals("02") ) {
								month = "08";
							}
							else if ( month.equals("03") ) {
								month = "11";
							}
							else if ( month.equals("04") ) {
								month = "02";
							}
						}
						else if ( reportId == 3 ) {
							if ( month.equals("01") ) {
								month = "03";
							}
							else if ( month.equals("02") ) {
								month = "06";
							}
							else if ( month.equals("03") ) {
								month = "09";
							}
							else if ( month.equals("04") ) {
								month = "12";
							}
						}
						else if ( reportId == 7 ) {
							if ( month.equals("01") ) {
								month = "01";
							}
							else if ( month.equals("02") ) {
								month = "04";
							}
							else if ( month.equals("03") ) {
								month = "07";
							}
							else if ( month.equals("04") ) {
								month = "10";
							}
						}
					}
					logger.debug(jsonElObj.get("period").getAsString() );
					
					int sid = getSchedId(year, month, reportId);
					er.setSchedId(sid);
					er.insert();
				}
			}
		}
		else {
			throw new BlsException("Results cannot be processed: " + je );
		}
	}
	
	/**
	 * Updates the EcoReport tables with the price deltas for the day as a percent
	 * 
	 * @throws BlsException - for database issues
	 */
	private void updateDeltas() throws BlsException {
		
		List<EcoReport> reports = EcoReport.getAllReports();
		for (EcoReport report : reports ) {
			EcoReport previousReport = report.getPreviousEcoReport();
			if ( previousReport != null ) {
				float c = Math.round(((report.getValue() - previousReport.getValue())/previousReport.getValue()) *10000);	
				report.setDelta(c/100);
			}
			else {
				report.setDelta(0);
			}
			report.updateDelta();
		}
		
		
		
	}
	
	private int getSchedId(String year, String month, int reportId) throws BlsException {
		int id = SchedCode.findId(year, month, reportId);
		return id;
	}

	private int getParentId(String code) throws BlsException {
		int parent = (Integer) BlsSubReport.getAllCodesAsMap().get(code) ;
		return parent;
	}

	private int getReportId(String code) throws BlsException {
		int parent = (Integer) BlsSubReport.getAllCodesAsReportCodeMap().get(code) ;
		return parent;
	}

	@Override
	public void destroy() {
		//nothing to null out
	}

	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */	
	public static void main(String[] args) {
		
		EcoReportData erd = new EcoReportData();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		try {
			erd.process();
		} catch (BlsException e) {
			logger.error(e);
		}
	}
	
}


