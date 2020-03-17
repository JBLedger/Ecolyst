package com.ecolyst.upload.common;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession ;

public class SparkConnection {
	
	static {
		Logger.getLogger("org").setLevel(Level.INFO);
		Logger.getLogger("akka").setLevel(Level.INFO);
	}
	Logger logger = Logger.getLogger("com.blsProcessor");
	

	//A name for the spark instance. Can be any string
	private static String appName = "Ecolyst";
	//Pointer / URL to the Spark instance - embedded
	private static String sparkMaster = "local[2]";
	
	private static JavaSparkContext spContext = null;
	private static SparkSession sparkSession = null;
	private static String tempDir = "file:///c:/temp/spark-warehouse";
	
	private static void getConnection() {
		
		if ( spContext == null) {	
			//Setup Spark configuration
			SparkConf conf = new SparkConf()
					.setAppName(appName)
					.setMaster(sparkMaster);
			
			//Make sure you download the winutils binaries into this directory
			//from https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
			System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");	
			
			//Create Spark Context from configuration
			spContext = new JavaSparkContext(conf);
			
			 sparkSession = SparkSession
					  .builder()
					  .appName(appName)
					  .master(sparkMaster)
					  .config("spark.sql.warehouse.dir", tempDir)
					  
					  .config("url", "jdbc:mysql://localhost:3306/ecolyst")
						.config("driver", "com.mysql.jdbc.Driver")
						.config("user", "root")
						.config("password", "password")
						.config("characterEncoding", "latin1")

					  .getOrCreate();
			 
		}
		
	}
	
	public static JavaSparkContext getContext() {
		
		if ( spContext == null ) {
			getConnection();
		}
		return spContext;
	}
	
	public static SparkSession getSession() {
		if ( sparkSession == null) {
			getConnection();
		}
		return sparkSession;
	}

}
