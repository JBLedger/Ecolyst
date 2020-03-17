package com.ecolyst.analysis.spark;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import com.ecolyst.analysis.PivotTables;
import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.common.SparkConnection;
import com.ecolyst.upload.dal.BlsReport;
import com.ecolyst.upload.dal.Singler;


/**
 * Regression Design
-----------------
get all tables with names that start with gnr_
for each table
	create a (Spark) dataframe on all rows
	for each distinct stock id in the dataframe
		create a dataframe for that stock
		run regression
		store results
	end for
end for

 * @author LedgerZen
 *
 */

public class SingleRegression {
	
	static {
		Logger.getLogger("org").setLevel(Level.INFO);
		Logger.getLogger("akka").setLevel(Level.INFO);
	}
	static Logger logger = Logger.getLogger("com.blsProcessor");

	Connection conn = null;
	static Map<String,String> jdbcOptions;
	static {
		jdbcOptions = new HashMap<String,String>();
		jdbcOptions.put("url", "jdbc:mysql://localhost:3306/ecolyst");
		jdbcOptions.put("driver", "com.mysql.jdbc.Driver");
		jdbcOptions.put("user", "root");
		jdbcOptions.put("password", "password");
		jdbcOptions.put("characterEncoding", "latin1");
	}
	
	private void computeSingleVariableRegression() throws BlsException {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		for ( String tableName : getGeneratedTableNames() ) {
			Dataset<Row> genericDataTable = getSparkDbTable(tableName);
			logger.debug("table name: " + tableName + " : " + genericDataTable.count() );
			Dataset<Row> symbols = genericDataTable.select("stock_id").distinct();
			for ( Row symbol : symbols.collectAsList() ) {
				Dataset<Row> allRowsBySymbol =  genericDataTable.filter("stock_id = " + (Integer)symbol.getAs(0));
				if ( allRowsBySymbol.count() > 10 ) { //arbitrary minimum rows for ml
					logger.debug("table name: " + tableName + " : " + genericDataTable.count()  + " : Symbol: " + (Integer)symbol.getAs(0) + " : " + allRowsBySymbol.count());

					for ( StructField field : allRowsBySymbol.schema().fields() ) {
						if ( field.name().startsWith("sr_") ) {
							Double d = allRowsBySymbol.stat().corr("stock_delta", field.name());
							Singler s = new Singler();
							s.setReportTypeId(1);
							s.setReportId(BlsReport.getReportCodes().get(tableName.substring(4)));
							s.setParentId(Integer.parseInt(field.name().substring(3)));
							s.setStockId(symbol.getAs(0));
							s.setNbrRecs(allRowsBySymbol.count());
							s.setR2(d);
							s.insert();
						}
					}
				}
			}
			
		}
		
	}
	
	private List<String> getGeneratedTableNames() throws BlsException {
		
		return PivotTables.getPivotTables();
		
	}
	
	private Dataset<Row> getSparkDbTable(String tableName) {
		
		SparkSession spark = SparkConnection.getSession();
		jdbcOptions.put("dbtable", tableName);
		DataFrameReader ds = spark.read().format("jdbc").options(jdbcOptions);
		Dataset<Row> dd = ds.load().toDF();
		return dd;

	}
	

	public static void main(String[] args) {
		
		SingleRegression reg = new SingleRegression();
		try {
			reg.computeSingleVariableRegression();
		} catch (BlsException e) {
			logger.error("Could not perform regression", e);
		}
		
	}

	
}
