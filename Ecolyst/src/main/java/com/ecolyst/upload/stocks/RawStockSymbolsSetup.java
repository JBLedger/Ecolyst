package com.ecolyst.upload.stocks;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.BlsSchedSetup;
import com.ecolyst.upload.bls.IStep;
import com.ecolyst.upload.bls.StockSymbol;

/**
 * Loads all the stock market symbols from a file.
 * 
 * Note - currently this is just stocks from the NYSE
 * 
 * @author LedgerZen
 *
 */
public class RawStockSymbolsSetup implements IStep {
	
	private final static String rgx = "\"(.*?)\",\"(.*?)\",\".*?\",\".*?\",\".*?\",\"(.*?)\",\"(.*?)\".*?";
	
	public void process() throws BlsException {
		
		String contents = loadStockFile();
		
		Pattern pattern = Pattern.compile( rgx );
		Matcher m = pattern.matcher(contents);
		String symbol = null;
		String name = null;
		String sector = null;
		String subSector = null;

		while (m.find()) {
	    	symbol = m.group(1);
	    	name = m.group(2);
	    	sector = m.group(3);
	    	subSector = m.group(4);
			if ( symbol != null && symbol.trim().length() > 0 ) {
				saveSymbol(symbol, name, sector, subSector);
			}
		}
	}
	
	private void saveSymbol(String symbol, String name, String sector, String subSector) throws BlsException {

		StockSymbol ss = new StockSymbol();
		ss.setSymbol(symbol);
		ss.setName(name);
		ss.setSector(sector);
		ss.setSubSector(subSector);
		ss.insert();
		logger.debug(symbol + ": " + name);
		
	}
	
	
	static String loadStockFile() throws BlsException {
		
		byte[] me = null;
		String filename = "StockSymbols.txt";
		try {
			InputStream in = BlsSchedSetup.class.getClassLoader().getResourceAsStream(filename);
			me = new byte[in.available()];
			in.read(me);
		}
		catch (IOException ioe) {
			throw new BlsException("Cannot load file: " + filename + " : " + ioe.getLocalizedMessage());
		}
		return new String(me);
	}

	@Override
	public void destroy() {
		//nothing to do
	}

	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */	
	public static void main(String[] args) {
		
		RawStockSymbolsSetup rsss = new RawStockSymbolsSetup();
		Logger.getRootLogger().setLevel(Level.DEBUG);

		try {
			rsss.process();
		} catch (BlsException e) {
			logger.error(e);
		}

	}

}
