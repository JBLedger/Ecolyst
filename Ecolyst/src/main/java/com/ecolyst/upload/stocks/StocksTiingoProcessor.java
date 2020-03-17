package com.ecolyst.upload.stocks;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.IStep;
import com.ecolyst.upload.bls.StockSymbol;
import com.ecolyst.upload.common.Constants;
import com.ecolyst.upload.common.Utils;
import com.ecolyst.upload.dal.Quotes;
import com.ecolyst.upload.dal.SchedCode;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class StocksTiingoProcessor implements IStep {

	/**
	 * Format the url with the correct start/end dates
	 */
	private String enddate = Quotes.SDF.format(new Date());	
	private String key = Constants.TIINGO_KEY;
	private String strUrl = "https://api.tiingo.com/tiingo/daily/{symbol}/prices?startDate={startdate}&endDate={enddate}&resampleFreq=daily&token={key}".replace("{enddate}", enddate).replace("{key}", key);
	
	/**
	 * Processes getting quotes from Tiingo
	 * Processing is done by 
	 *  - getting all stocks
	 *   - then for each stock check to see if there's missing quotes
	 *   - if there's missing quotes, go to tiingo to get all data back to 2015
	 *   - add the missing data
	 *   
	 * Future work:
	 *  - only get quotes from Tiingo back to the oldest missing data, not 2015
	 *    there's no $ cost benefit to do this, just processing consumption and time
	 *   
	 */
	public void process() throws BlsException {
		
		List<StockSymbol> symbols = StockSymbol.getAllStocks();

		for ( StockSymbol symbol : symbols ) {
			logger.debug("Symbol: " + symbol.getSymbol());
			Map<String, SchedCode> missingQuotes = Quotes.getNeededSchedCodesMapBySymbol(symbol.getSymbol());
			if ( missingQuotes.size() > 0 ) {
				String startDate = Quotes.SDF.format( missingQuotes.get(missingQuotes.keySet().iterator().next()).getSched_date() );
				Map<String, Quotes> qMap = getTiingoQuotes(symbol.getSymbol(), startDate);
				for ( String sckey : missingQuotes.keySet() ) {
					SchedCode sc = missingQuotes.get(sckey);
					String qDate = Quotes.SDF.format( sc.getSched_date() );
					Quotes q = qMap.get(qDate);

					//quotes may not exist from tiingo, but they should be added to our db regardless and keep the values null.
					if ( q == null ) {
						q = new Quotes();
					}
					else {
						float c = Math.round(((q.getClose() - q.getOpen())/q.getOpen()) *10000);	
						q.setDelta(c/100);
					}
					q.setSchedId(missingQuotes.get(qDate).getId());
					q.setStockSymbolId(symbol.getId());
					q.insert();
				}
			}
		}
	}

	/**
	 * Performs the API call the Tiingo to get market data
	 * 
	 * @param symbol The stock symbol for which to get quotes
	 * @return A map by date of quotes for the given symbol
	 * @throws BlsException This wraps SQL exceptions
	 */
	private Map<String, Quotes> getTiingoQuotes(String symbol, String startdate) throws BlsException {

		String URL = strUrl.replace("{symbol}",symbol.trim()).replace("{startdate}", startdate);
		logger.debug(URL);
		JsonElement json = null;
		try {
			URL url = new URL(URL);
			URLConnection connection = url.openConnection();

	        InputStream is = connection.getInputStream();
	        json = Utils.getJsonElement(is);
		} catch (IOException e) {
			logger.error("Problem with Tiinga WS call, symbol: " + symbol, e);
		}
		return parseOutDatesAndQuotes(json);
	}
	
	/**
	 * Parses the Json quote received from Tiingo into a map by date of quotes
	 * 
	 * @param json The Json from Tiingo to parse
	 * @return The java version of the parsed Json as map by String date and Quotes
	 */
	private Map<String, Quotes> parseOutDatesAndQuotes(JsonElement json) {
		
		Map<String, Quotes> map = new HashMap<String, Quotes>();
		
		if ( json != null ) {
			JsonArray myjsonArray = json.getAsJsonArray();
			if ( myjsonArray != null ) {
				
				for ( JsonElement jResults : myjsonArray ) {
					JsonObject je = jResults.getAsJsonObject();
					
					Quotes q = new Quotes();
					q.setOpen(je.get("adjOpen").getAsFloat());
					q.setClose(je.get("adjClose").getAsFloat());
					q.setHigh(je.get("adjHigh").getAsFloat());
					q.setLow(je.get("adjLow").getAsFloat());
					String quoteDate = je.get("date").getAsString().substring(0,10);
					map.put(quoteDate, q);
				}
			}
			else {
				logger.error("Issue with return element:\n" + json.getAsJsonObject());
			}
		}
		return map;
	}


	@Override
	public void destroy() {
		enddate = null;
		strUrl = null;
		key = null;
	}

	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */		
	public static void main (String[] args) {
		
		StocksTiingoProcessor stp = new StocksTiingoProcessor();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		try {
			stp.process();
		} catch (BlsException e) {
			logger.error("main error", e);
		}
		
		
	}
	
}
