package com.ecolyst.upload.dal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.Utils;

public class Quotes {
	
	static Logger logger = Logger.getLogger("com.blsProcessor");

	public static SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd");


	private int id;
	private int schedId;
	private int stockSymbolId;
	private float open;
	private float close;
	private float delta;
	private float high;
	private float low;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getSchedId() {
		return schedId;
	}
	public void setSchedId(int schedId) {
		this.schedId = schedId;
	}
	public int getStockSymbolId() {
		return stockSymbolId;
	}
	public void setStockSymbolId(int stockSymbolId) {
		this.stockSymbolId = stockSymbolId;
	}
	public float getOpen() {
		return open;
	}
	public void setOpen(float open) {
		this.open = open;
	}
	public float getClose() {
		return close;
	}
	public void setClose(float close) {
		this.close = close;
	}
	public float getDelta() {
		return delta;
	}
	public void setDelta(float delta) {
		this.delta = delta;
	}
	

	public float getHigh() {
		return high;
	}
	public void setHigh(float high) {
		this.high = high;
	}
	public float getLow() {
		return low;
	}
	public void setLow(float low) {
		this.low = low;
	}
	
	
	/**
	 * Returns a list of SchedCode's where the key is the date.
	 * It only returns dates that are missing from the Quotes table
	 * 
	 * @param symbol - the symbol for the missing sched codes
	 * @return A map by string of SchedCode's
	 * @throws BlsException  The BlsException wraps SQLExceptions
 	 */
	public static Map<String, SchedCode> getNeededSchedCodesMapBySymbol(String symbol) throws BlsException {
		
		Map<String, SchedCode> codes = new HashMap<String, SchedCode>();
		String sql = "select id, sched_date, report_type_id, report_id from sched_dates where id not in ( " + 
				"select sched_id from quotes where stock_symbol_id = ( " + 
				"select id from stock_symbols where symbol = ?)) " +
				"and sched_date < now()" +
				"order by 1";
		
		Connection con = Utils.getDbConnection();
		Date now = new Date();
		
		try {
			PreparedStatement ps = con.prepareStatement(sql);
			ps.setString(1, symbol);
			ResultSet rs = ps.executeQuery();
			while (rs.next() ) {
				
				Date schedDate = rs.getDate(2);
				if ( schedDate.getTime() < now.getTime() ) {
					
					SchedCode sc = new SchedCode();
					sc.setId(rs.getInt(1));
					sc.setSched_date(rs.getDate(2));
					sc.setReport_type_id(rs.getInt(3));
					sc.setReport_id(rs.getInt(4));
					codes.put(SDF.format(sc.getSched_date()), sc);
				}
			}
		}
		catch (SQLException se) {
			throw new BlsException ("Issues getting sched codes map by symbol", se);
		}
		return codes;
	}
	
	
	public static void populateCleanQuotes() throws BlsException  {

		String sql = "insert into clean_quotes (select * from quotes where id not in (select id from clean_quotes) and id not in (select id from quotes where open=close and open=low and open=high))" ;
		Connection con = Utils.getDbConnection();
		try {
			PreparedStatement pstate = con.prepareStatement(sql);
			pstate.executeUpdate();
		} catch (SQLException e) {
			throw new BlsException("Issues populating clean quotes", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws BlsException
	 */
	public int insert() throws BlsException {
		
		String sql = "insert into quotes ( stock_symbol_id, sched_id, open, high, low, close, delta ) select  ?, ?, ?, ?, ?, ?, ? where not exists (select 1 from quotes where stock_symbol_Id = ? and sched_id = ?)";
		Connection con = Utils.getDbConnection();
		int id = -1;
		try {
			PreparedStatement pstate = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, getStockSymbolId());
			pstate.setInt(2, getSchedId());
			pstate.setFloat(3, getOpen());
			pstate.setFloat(4, getHigh());
			pstate.setFloat(5,  getLow());
			pstate.setFloat(6, getClose());
			pstate.setFloat(7, getDelta());
			pstate.setInt(8, getStockSymbolId());
			pstate.setInt(9, getSchedId());
			pstate.executeUpdate();

	        try (ResultSet generatedKeys = pstate.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                logger.debug("Inserting into Quotes failed, duplicate detected.");
	    			PreparedStatement psel = con.prepareStatement("select 1 from quotes where stock_symbol_Id = ? and sched_id = ?");
	    			psel.setInt(1, getStockSymbolId());
	    			psel.setInt(2, getSchedId());
	    			ResultSet rs = psel.executeQuery();
	    			if ( rs.next() ) {
	    				id = rs.getInt(1);
	    			}
	    			else {
	    				throw new BlsException("Duplicate quote causing issues, stock symbol id: " + getStockSymbolId() + " : Schedule id: " + getSchedId());
	    			}
	            }
	        }
		} catch (SQLException e) {
			throw new BlsException("Issues saving to Quotes", e);
		}
		return id;
		
	}
	
	public static boolean doQuotesForThisSymbolNeedToBeDownloaded(String symbol) throws BlsException {
		
		boolean doesIt = true;
		String sql = "select q.id as qid, open, close from Quotes q, stock_symbols ss where symbol = '"+symbol+"' and ss.id= q.stock_symbol_id";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next() ) {
				float open = rs.getFloat(2);
				float close = rs.getFloat(3);
				if ( Math.round(open*10) == 0 || Math.round(close*10)==0 ) {
					deleteQuote(rs.getInt(1));
				}
				else {
					doesIt = false;
				}
			}
		} catch (SQLException e) {
			throw new BlsException("Issues with query for quote existence", e);
		}
		return doesIt;
	}
	
	public static boolean doQuotesForThisSymbolExist(String symbol) throws BlsException {
		
		boolean doesIt = false;
		String sql = "select '1' from Quotes q, stock_symbols ss where symbol = '"+symbol.trim()+"' and ss.id=q.stock_symbol_id and symbol = '"+symbol+"' limit 1";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			if (rs.next() ) {
				doesIt = true;
			}
			else {
				System.out.println("doesn't exist: " + symbol);
			}
		} catch (SQLException e) {
			throw new BlsException("Unable to perform quote query", e);
		}
		return doesIt;
	}

	public static Map<Integer, Map<Integer, Quotes>> getCleanQuotesMapBySchedIdStockId() throws BlsException {
		
		//sched id, stock_id
		Map<Integer, Map<Integer, Quotes>> quotes = new HashMap<Integer, Map<Integer, Quotes>>();
		
		String sql = "select id, sched_id, stock_symbol_id, open, close, delta, low, high from clean_quotes";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next() ) {
				Quotes quote = new Quotes();
				quote.setId(rs.getInt(1));
				quote.setSchedId(rs.getInt(2));
				quote.setStockSymbolId(rs.getInt(3));
				quote.setOpen(rs.getFloat(4));
				quote.setClose(rs.getFloat(5));
				quote.setDelta(rs.getFloat(6));
				quote.setLow(rs.getFloat(7));
				quote.setHigh(rs.getFloat(8));
				Map<Integer, Quotes> schedMap = quotes.get(quote.getSchedId());
				if ( schedMap == null ) {
					schedMap = new HashMap<Integer, Quotes>();
					quotes.put(quote.getSchedId(), schedMap);
				}
				schedMap.put(quote.getStockSymbolId(), quote);
			}
		} catch (SQLException e) {
			throw new BlsException("Issues encountered getting map of quote data", e);
		}
		return quotes;
	}


	public static List<List<Quotes>> getCleanQuotesListsByStockId() throws BlsException {
		
		// stock id, sched id
		List<List<Quotes>> quotes = new ArrayList<List<Quotes>>();
		List<Quotes> currentList = null;
		
		String sql = "select id, sched_id, stock_symbol_id, open, close, delta, low, high from clean_quotes order by stock_symbol_id, sched_id";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			int currentStockId = -1;
			while (rs.next() ) {
				Quotes quote = new Quotes();
				quote.setId(rs.getInt(1));
				quote.setSchedId(rs.getInt(2));
				quote.setStockSymbolId(rs.getInt(3));
				quote.setOpen(rs.getFloat(4));
				quote.setClose(rs.getFloat(5));
				quote.setDelta(rs.getFloat(6));
				quote.setLow(rs.getFloat(7));
				quote.setHigh(rs.getFloat(8));
				if ( quote.getStockSymbolId() != currentStockId ) {
					currentList = new ArrayList<Quotes>();
					quotes.add(currentList);
				}
				currentList.add(quote);
			}
		} catch (SQLException e) {
			throw new BlsException("Issues encountered getting map of quote data", e);
		}
		return quotes;
	}

	public static void deleteQuote(int id) throws SQLException, BlsException {
		
		Connection con = Utils.getDbConnection();
		PreparedStatement ps = con.prepareStatement("delete from Quotes where id = ?");
		ps.setInt(1, id);
		ps.execute();
	}

}
