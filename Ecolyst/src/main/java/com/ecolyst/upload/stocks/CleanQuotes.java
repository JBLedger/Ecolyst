package com.ecolyst.upload.stocks;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ecolyst.upload.bls.BlsException;
import com.ecolyst.upload.bls.IStep;
import com.ecolyst.upload.dal.Quotes;



/**
 * This class has one cleaning mechanism.  If more are needed, add new IStep(s)
 * 
 * @author LedgerZen
 *
 */
public class CleanQuotes implements IStep {

	/**
	 * If new clean methods are needed, create new ISteps.
	 * 
	 * @throws BlsException
	 */
	public void process() throws BlsException {
		//all work can be done with sql scripts, so no Java logic is necessary
		Quotes.populateCleanQuotes();
	}

	@Override
	public void destroy() {
		//no instance variables to deal with
	}

	/**
	 * This is here for testing/development
	 * 
	 * This class is idempotent, meaning it can be run many times without changing state
	 * 
	 * @param args
	 */		
	public static void main(String[] args) {
		
		CleanQuotes cq = new CleanQuotes();
		Logger.getRootLogger().setLevel(Level.DEBUG);
		
		try {
			cq.process();
		}
		catch(BlsException be) {
			logger.error(be);
		}
	}
}
