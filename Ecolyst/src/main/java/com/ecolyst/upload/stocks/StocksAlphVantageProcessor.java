package com.ecolyst.upload.stocks;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.StockSymbol;
import com.ecolyst.upload.common.Utils;
import com.ecolyst.upload.dal.Quotes;
import com.ecolyst.upload.dal.SchedCode;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class StocksAlphVantageProcessor {

	Logger logger = Logger.getLogger("com.blsProcessor");

	String[] alphaKeys = {"abc","123"};

	private void processAlphaVantageDayPrices(List<SchedCode> schedCodes) throws BlsException {
		List<StockSymbol> symbols = StockSymbol.getAllStocks();
		  
//		Map<String, StockSymbol> symbols = new HashMap<String, StockSymbol>();
//		StockSymbol ss = new StockSymbol();
//		ss.setId(23);
//		ss.setSymbol("ACEL");
//		symbols.put("ACEL", ss);
		
		
		Map<String, SchedCode> scheds = getSchedCodesAsMap(schedCodes);
		
		int currentKeyIndex = 0;
		int loopCount = 0;
		for ( StockSymbol symbol : symbols ) {
			//iterate through the different keys, 500 per key
			if (loopCount++ >= 500 ) {
				currentKeyIndex++;
				loopCount = 0;
			}
			//wait 12 seconds - limitation of AlphaVantage of 5 gets/minute
			wait12();
			Map<String, Quotes> qMap = getAlphaQuotes(symbol.getSymbol(), alphaKeys[currentKeyIndex]);
			for ( String qDate : qMap.keySet() ) {
				if ( scheds.get(qDate) != null ) {
					Quotes q = qMap.get(qDate);
					q.setSchedId(scheds.get(qDate).getId());
					q.setStockSymbolId(symbol.getId());
					float c = Math.round(((q.getClose() - q.getOpen())/q.getOpen()) *10000);	
					q.setDelta(c/100);
					q.insert();
				}
			}
		}
	}
	
	private static void wait12() {
		try {
			TimeUnit.SECONDS.sleep(12);
		} catch (InterruptedException e) {
			// this will never happen, unless someone changes the code
		}
	}
	
	private Map<String, SchedCode> getSchedCodesAsMap(List<SchedCode> schedCodes) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Map<String, SchedCode> map = new HashMap<String, SchedCode>();
		for ( SchedCode code : schedCodes ) {
			map.put(sdf.format(code.getSched_date()), code);
		}
		return map;
	}
	
	private Map<String, Quotes> getAlphaQuotes(String symbol, String key) throws BlsException {

		String strUrl = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=".replace("{symbol}",symbol.trim()) + key + "&outputsize=full";
		JsonElement json = null;
		try {
			URL url = new URL(strUrl);
			URLConnection connection = url.openConnection();

	        InputStream is = connection.getInputStream();
	        json = Utils.getAlphaJsonElement(is);
		} catch (IOException e) {
			throw new BlsException ("Problem with AlphaVantage WS call: " + e.getLocalizedMessage());
		}
		return parseOutDatesAndQuotes(json);
	}
	
	private Map<String, Quotes> parseOutDatesAndQuotes(JsonElement json) {
		
		Map<String, Quotes> map = new HashMap<String, Quotes>();
		
		JsonObject jo = json.getAsJsonObject();
		JsonObject jResults = jo.getAsJsonObject("Time Series (Daily)");
		if ( jResults != null ) {
			Set<String> keys = jResults.keySet();
			
			for ( String strJson : keys ) {
				JsonObject je = jResults.getAsJsonObject(strJson);
				float open = je.get("1. open").getAsFloat();
				float high = je.get("2. high").getAsFloat();
				float low =  je.get("3. low").getAsFloat();
				float close = je.get("4. close").getAsFloat();
				Quotes q = new Quotes();
				q.setOpen(open);
				q.setClose(close);
				q.setHigh(high);
				q.setLow(low);
				map.put(strJson, q);
			}
		}
		else {
			logger.debug("Issue with return element:\n" + json.getAsJsonObject());
		}
		return map;
	}
	

	
	/**
	 * This should come from a database table
	 * @param args
	 */
//	private List<StockSymbol> getSymbols() {
//		
//		return StockSymbol.getAllUnprocessedNyseStocksBySymbol();
//	}
	
	/**
	 * This represents the date and time of the report release
	 * 
	 * This should come from a database table
	 * @param args
	 */
	private List<SchedCode> getReportStartDates() throws BlsException {
		
		return SchedCode.findAll();
	}
	
	
	public static void main (String[] args ) throws BlsException {
		
		StocksAlphVantageProcessor s = new StocksAlphVantageProcessor();
		List<SchedCode> schedCodes = s.getReportStartDates();
		try {
			s.processAlphaVantageDayPrices(schedCodes);
		} catch (BlsException e) {
			s.logger.error(e);
		}
		
	}
}
