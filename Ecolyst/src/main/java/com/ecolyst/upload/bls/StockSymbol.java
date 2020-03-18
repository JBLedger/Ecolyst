package com.ecolyst.upload.bls;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.ecolyst.upload.common.Utils;

public class StockSymbol {
	
	static Logger logger = Logger.getLogger("com.blsProcessor");
	
	private static List<StockSymbol> list = null;

	private int id;
	private int market_id;
	private String symbol = null;
	private String name = null;
	private String sector = null;
	private String subSector = null;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	
	public int getMarketId() {
		return market_id;
	}
	public void setMarketId(int market_id) {
		this.market_id = market_id;
	}

	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSector() {
		return sector;
	}
	public void setSector(String sector) {
		this.sector = sector;
	}
	public String getSubSector() {
		return subSector;
	}
	public void setSubSector(String subSector) {
		this.subSector = subSector;
	}
	
	public int insert() throws BlsException {
		
		String sql = "insert into stock_symbols (market_id, symbol, name, sector, subsector) select ?,?,?,?,? where not exists (select 1 from stock_symbols where market_id = 1 and symbol = ?)";
		Connection con = Utils.getDbConnection();
		int id = -1;
		try {
			PreparedStatement pstate = con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			pstate.setInt(1, 1);
			pstate.setString(2, symbol);
			pstate.setString(3, name);
			pstate.setString(4, sector);
			pstate.setString(5, subSector);
			pstate.setString(6, symbol);
			pstate.executeUpdate();
	        try (ResultSet generatedKeys = pstate.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	                id = generatedKeys.getInt(1);
	            }
	            else {
	                logger.debug("Could not insert into stock_symbols, duplicate detected: symbol: " + symbol );
	    			PreparedStatement psel = con.prepareStatement("select id from stock_symbols where market_id = 1 and symbol = ?");
	    			psel.setString(1, symbol);
	    			ResultSet rs = psel.executeQuery();
	    			if ( rs.next() ) {
	    				id = rs.getInt(1);
	    			}
	    			else {
	    				throw new BlsException("Duplicate stock symbol causing issues: " + symbol);
	    			}
	            }
	        }
			
		} catch (SQLException e) {
			throw new BlsException("Cannot insert StockSymbols: " + e.getLocalizedMessage());
		}
		return id;
	}
	
//	public static List<StockSymbol> getAllUnprocessedNyseStocksBySymbol() throws BlsException {
//		
//		if ( upprocessedlist == null ) {
//			upprocessedlist = new ArrayList<StockSymbol>();
//			String sql = "select id, symbol from stock_symbols where symbol not like '%^%' and symbol not like '%.%' and symbol not like '%$%' and symbol not like '%~%' and symbol not like '%Symbol%'";// and id not in (select stock_symbol_id from Quotes)" ;
//			Connection con = Utils.getDbConnection();
//			try {
//				Statement stmt = con.createStatement();
//				ResultSet rs = stmt.executeQuery(sql);
//				while (rs.next() ) {
//					StockSymbol ss = new StockSymbol();
//					ss.setId(rs.getInt(1));
//					ss.setSymbol(rs.getString(2));
//					upprocessedlist.add(ss);
//				}
//			} catch (SQLException e) {
//				logger.error(e);
//			}
//		}
//		return upprocessedlist;
//	}
	
	
	public static List<StockSymbol> getAllStocks() throws BlsException {
		
		if ( list == null ) {
			list = new ArrayList<StockSymbol>();
			String sql = "select id, symbol from stock_symbols where symbol not like '%^%' and symbol not like '%.%' and symbol not like '%$%' and symbol not like '%~%' and symbol not like '%Symbol%'" ;
			Connection con = Utils.getDbConnection();
			try {
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next() ) {
					StockSymbol ss = new StockSymbol();
					ss.setId(rs.getInt(1));
					ss.setSymbol(rs.getString(2));
					list.add(ss);
				}
			} catch (SQLException e) {
				logger.error(e);
				throw new BlsException("Issues encountered while querying stocks");
			}
		}
		return list;
	}
	
	
	public static void populateAvgDeltas() throws BlsException {
		
		String selectSql = "select stock_symbol_id, round(avg(delta),3), round(avg(abs(delta)),3) from clean_quotes group by stock_symbol_id";
		String updateSql = "update stock_symbols set delta_avg = ?, delta_avg_abs = ? where id = ?";
		Connection con = Utils.getDbConnection();
		try {
			Statement stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(selectSql);
			PreparedStatement pstate = con.prepareStatement(updateSql);
			while (rs.next() ) {
				pstate.setFloat(1, rs.getFloat(2));
				pstate.setFloat(2, rs.getFloat(3));
				pstate.setInt(3, rs.getInt(1));
				pstate.executeUpdate();
			}
		} catch (SQLException e) {
			logger.error(e);
			throw new BlsException("Issues encountered while calculating deltas");
		}
	}


}
