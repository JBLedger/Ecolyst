package com.ecolyst.upload.bls;

import org.apache.log4j.Logger;

/**
 * This interface represents a single processing step
 * 
 * This classes that implement this interface should be idempotent, 
 * meaning it can be run many times without changing state, it helps
 * with testing and development
 * 
 * @author LedgerZen
 *
 */
public interface IStep {

	/**
	 * This is static
	 */
	Logger logger = Logger.getLogger("com.blsProcessor");
	
	
	/**
	 * Starts all processing for this step
	 */
	public void process() throws BlsException;
	
	/**
	 * Clean up any variables that should be nulled
	 */
	public void destroy();
	
}
