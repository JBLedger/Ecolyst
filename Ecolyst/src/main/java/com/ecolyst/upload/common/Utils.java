package com.ecolyst.upload.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class Utils {

	static Logger logger = Logger.getLogger("com.blsProcessor");
	
	
	private static Connection con = null;

	public static String getUrlContents(String url) throws BlsException {
			
		HttpPost httpPost = new HttpPost(url);
        HttpClientBuilder client = HttpClientBuilder.create();
        InputStream is = null;
        try {
            HttpResponse response = client.build().execute(httpPost);
			is = response.getEntity().getContent();
		} catch (UnsupportedOperationException | IOException e) {
			throw new BlsException("Issues encountered while getting url contents", e);
		}
        
        BufferedReader in = new BufferedReader( new InputStreamReader(is) );
        String current;
        String result = null;
        try {
			while((current = in.readLine()) != null) {
			   result += current;
			}
		} catch (IOException e) {
			throw new BlsException("Issues encountered while compiling url contents", e);
		}
        logger.debug(result);
        
        return result;
		
	}
	
	public static Connection getDbConnection() throws BlsException {
		
		if ( con == null ) {
			try {
				con=DriverManager.getConnection(  
				"jdbc:mysql://localhost:3306/ecolyst?characterEncoding=latin1","root","password");  
			}
			catch(Exception e){ 
					throw new BlsException("Cannot get connection to the database: ", e);
			}
		}
		return con;
	}
	
	public static JsonElement getAlphaJsonElement(InputStream is) throws BlsException {
        BufferedReader in = new BufferedReader( new InputStreamReader(is) );
        String current;
        String result = null;
        try {
			while((current = in.readLine()) != null) {
			   result += current;
			}
		} catch (IOException e) {
			throw new BlsException("Cannot get data from web service: ", e);
		}
        
        JsonParser parser = new JsonParser();
        JsonElement je = parser.parse(result.substring(4));
        return je;
        
	}

	public static JsonElement getJsonElement(InputStream is) throws BlsException {
        BufferedReader in = new BufferedReader( new InputStreamReader(is) );
        String current;
        String result = null;
        try {
			while((current = in.readLine()) != null) {
			   result += current;
			}
		} catch (IOException e) {
			throw new BlsException("Issues compiling json data", e);
		}
        
        JsonParser parser = new JsonParser();
        JsonElement je = null;
        try {
        	je = parser.parse(result.substring(4));
        }
        catch (Exception e) {
			throw new BlsException("Issues parsing json data", e);
        }
        return je;
        
	}
	
//	public static void hold() {
//		while (true) {
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//				logger.info("Interrupted sleep");
//			}
//		}
//	}
//	
//
//	public Utils() {
//		// TODO Auto-generated constructor stub
//	}

}
