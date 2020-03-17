package com.ecolyst.analysis.spark;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.IStep;
import com.ecolyst.upload.common.SparkConnection;
import com.ecolyst.upload.dal.BlsReport;
import com.ecolyst.upload.dal.Multipler;
import com.ecolyst.upload.dal.Predictions;


/**
 * Regression Design
   -----------------
get all tables with names that start with gnr_ (generated)
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

public class MultipleRegression implements IStep, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4319548288171469660L;
	
	//ensure Spark doesn't spew logs constantly
	static {
		Logger.getLogger("org").setLevel(Level.INFO);
		Logger.getLogger("akka").setLevel(Level.INFO);
	}
	
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
	
	/**
	 * Performs linear regression - stock price is the label compared with the eco sub reports
	 * 
	 *  Currently onl a subset of all the BLS reports are being processed, (cpi, ppi, ix, em).
	 *  Future work will need to include all the BLS reports
	 *  
	 *  If this is run more than once, the multipler db table will be update the columns
	 *  number of rows, coefficients, r2, and intercept 
	 *  
	 */
	public void process() throws BlsException {

		String[] mynames = {"gnr_cpi","gnr_ppi","gnr_ix","gnr_em"};
		for ( String tableName :  mynames ) {
			Dataset<Row> genericDataTable = getSparkDbTable(tableName);
			logger.info("table name: " + tableName + " : " + genericDataTable.count() );
			
			Dataset<Row> symbols = genericDataTable.select("stock_id").distinct();
			for ( Row symbol : symbols.collectAsList() ) {

				Dataset<Row> allRowsBySymbol =  genericDataTable.filter("stock_id = " + (Integer)symbol.getAs(0));
				if ( allRowsBySymbol.count() > 10 ) { //arbitrary minimum rows for ml
					
					Multipler mu = new Multipler();
					mu.setReportTypeId(1);
					mu.setReportId(BlsReport.getReportCodes().get(tableName.substring(4)));
					mu.setStockId((Integer)symbol.getAs(0));
					mu.setNbrRecs( (int) allRowsBySymbol.count() );
					
					logger.debug("table name: " + tableName + " : " + genericDataTable.count()  + " : Symbol: " + (Integer)symbol.getAs(0) + " : " + allRowsBySymbol.count());
					Dataset<Row>[] splits = getSplits(allRowsBySymbol);
					compute(splits, mu);
					mu.insert();
				}
			}
		}
		mynames = null;
	}
	
	private void compute(Dataset<Row>[] splits, Multipler mu) {
		LinearRegression lr = new LinearRegression();
		//Create the model
		LinearRegressionModel lrModel = lr.fit(splits[0]);
		
		//Print out coefficients and intercept for LR
		logger.debug("Coefficients: "
				  + lrModel.coefficients() + " Intercept: " + lrModel.intercept() );
		String coeffs = Arrays.toString(lrModel.coefficients().toArray()).substring(1).replace("]","");
		mu.setCoeffs( coeffs );
		mu.setIntercept( (float)(Math.round(lrModel.intercept() * 1000d) / 1000d) );
		
		logParms(lrModel);
		
		//Predict on test data
		Dataset<Row> predictions = lrModel.transform(splits[1]);
		
		//Compute R2 for the model on test data.
		double r2 = calcR2(predictions);
		logger.debug("R2 on test data = " + r2 + "\n\n");
		mu.setR2((float)r2);
		
		Dataset<Row> preds = predictions.select("label", "prediction","features");
		mu.setPredictions(getPredictions(preds));

		
	}

	private void logParms(LinearRegressionModel lrModel) {
		if ( logger.getLevel().equals( Level.DEBUG) ) {
			for ( Param<?> param : lrModel.params() ) {
				logger.debug(param.name() + ": " + param.doc() );
			}
		}
	}
	
	private List<Predictions> getPredictions(Dataset<Row> preds) {
		List<Predictions> muPreds = new ArrayList<Predictions>(); 
		for ( Row values : preds.collectAsList() ) {
			Predictions pred = new Predictions();
			muPreds.add(pred);
			pred.setLabel((float) values.getDouble(0) );
			pred.setPred((float) values.getDouble(1));
			DenseVector os =(DenseVector) values.get(2);
			int forCount = 0;
			for ( Double fv : os.values() ) {
				pred.setFeatures(pred.getFeatures() + fv);
				if ( ++forCount < os.values().length ) {
					pred.setFeatures(pred.getFeatures() + ",");
				}
			}
		}
		return muPreds;
	}
	
	private double calcR2( Dataset<Row> predictions) {

		RegressionEvaluator evaluator = new RegressionEvaluator()
				  .setLabelCol("label")
				  .setPredictionCol("prediction")
				  .setMetricName("r2");

		double r2 = -1;
		try {
			r2 = evaluator.evaluate(predictions);
		}
		catch (IllegalArgumentException ie) {
			logger.error("Bad values found while calculating r2", ie);
		}
		return r2;
	}
	
	private Dataset<Row>[] getSplits(Dataset<Row> dataset) {
		
		JavaRDD<Row> rdd3 = dataset.toJavaRDD().repartition(2);
		JavaRDD<LabeledPoint> rdd4 = rdd3.map( new Function<Row, LabeledPoint>() {

			private static final long serialVersionUID = 3000194403207531508L;
			@Override
			public LabeledPoint call(Row iRow) throws Exception {
				int rowLen = iRow.length() - 3;
				double[] v = new double[rowLen];
				for ( int i = 0; i < rowLen; i++ ) {
					v[i]=iRow.getDouble(i+3);
				}
				LabeledPoint lp = new LabeledPoint(iRow.getDouble(2) , Vectors.dense(v));
				return lp;
			}
		});
		SparkSession spark = SparkConnection.getSession();
		Dataset<Row> retval = spark.createDataFrame(rdd4, LabeledPoint.class);
		
		// Split the data into training and test sets (10% held out for testing).
		Dataset<Row>[] splits = retval.randomSplit(new double[]{0.7, 0.3});

		return splits;
		
	}
	
	
	private Dataset<Row> getSparkDbTable(String tableName) {
		
		SparkSession spark = SparkConnection.getSession();
		jdbcOptions.put("dbtable", tableName);
		DataFrameReader ds = spark.read().format("jdbc").options(jdbcOptions);
		Dataset<Row> dd = ds.load().toDF();
		return dd;

	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		
	}
	

	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */		
	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		Logger.getLogger("com").setLevel(Level.ERROR);

		MultipleRegression reg = new MultipleRegression();
		//Logger.getRootLogger().setLevel(Level.DEBUG);
		logger.setLevel(Level.ERROR);

		try {
			reg.process();
		} catch (BlsException e) {
			logger.error("Could not perform regression: ", e);
		}
	}

	
}
