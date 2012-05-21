package org.simplemessaging.fetching;

import org.apache.log4j.Logger;
import org.simplemessaging.*;
import org.simplemessaging.fetching.FetchState.Status;
import org.simplemessaging.util.AbstractSmartLifeCycle;

import java.util.List;


/**
 * Generic notification fetching (used for polling). Every potential notification fetching must extend this abstract class to properly
 * handle the fetch function and the default fetch state. A notification fetching reaches out to a CMS and checks to see what has changed
 * since the last time that it looked. It stores state so that it knows when the last time that it looked is.
 * 
 * @author RinconJ
 */
public abstract class MessageFetcher<T extends Message> extends AbstractSmartLifeCycle {
	
	private final static Logger logger = Logger
			.getLogger(MessageFetcher.class);
	
	private List<MessageListener<T>> messageListeners;
	
	private FetchState fetchState;
	
	private String fetchStateFile;
	
	/**
	 * Main method for fetching notifications
	 */
	public void fetch() throws Exception{
        if(!this.isRunning()){
            logger.info("This notification fetching '" + this.getClass().getName()+ "' is not running!");
            return;
        }
    	if(!this.isEnabled()){
    		logger.info("This notification fetching '" + this.getClass().getName()+ "' is disabled. Skipping task.");
    		return;
    	}
    		
     	logger.info("Running notification fetching " + this.getClass().getName());
        if(fetchState == null) { //try to load from properties file
            restoreFetchState();
        }
        
        do{
        	//copy fetch state
        	FetchState newFetchState = fetchState.copy();
        	newFetchState.setStatus(Status.CONTINUE);
        	T notification = fetch(newFetchState);

            notify(notification);
            //update fetch state
        	fetchState = newFetchState;
        	saveFetchState();
        }while(fetchState.getStatus()==Status.CONTINUE && isRunning());
        
        logger.info("Completed notification fetching " + this.getClass().getName());
	}

    public void notify(T notification) throws ListenerNotRunningException, ProcessorUnavailableException {
        if(notification != null && !notification.isEmpty()){
            for(MessageListener<T> listener: messageListeners)
                listener.receive(notification);
            postNotify(notification);
        }
    }

    /**
	 * Fetches notifications using the parameters specified in the fetchState
	 * fetchState.status should be updated to COMPLETED when no more items are found
	 * similarly other fetch parameters must be updated accordingly for the next fetch
	 * @param fetchState
	 * @return
	 * @throws Exception
	 */
	public abstract T fetch(FetchState fetchState) throws Exception;
	
	/**
	 * Generates a default fetch state. This will be used when the fetching can't restore the persisted state.
	 * @return
	 */
	public abstract FetchState defaultFetchState();

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;//A high value such that it's started last
    }

    /**
     * Placeholder method to do some post-processing after notification was delivered to all listeners
     * @param notification
     */
    protected void postNotify(T notification){

    }

    /**
     * Optional method
     * @param ids
     * @return
     */
    public T createNotification(List<String> ids){
        throw new RuntimeException("Method 'createNotification(List<String> ids):List' is not implemeted by this class:" + this.getClass());
    }

    /**
     * Restores the polling state from file
     */
    private void restoreFetchState() {
    	fetchState = null;
        try {
        	fetchState = FetchState.loadFrom(fetchStateFile);
        } catch (Exception e) {
            logger.warn("Failed loading polling state. Using defaults..." + this.getClass().getName(), e);
        } finally {
            if(fetchState == null){
            	fetchState = defaultFetchState();
            	fetchState.setStatus(Status.CONTINUE);
            }
        }
    }

    /**
     * Persists the polling state to file
     */
    private void saveFetchState() {
        try {
        	fetchState.saveTo(fetchStateFile);
        } catch (Exception e) {
            logger.error("Failed storing polling state.", e);
        }
    }

    public List<MessageListener<T>> getMessageListeners() {
        return messageListeners;
    }

    public void setMessageListeners(List<MessageListener<T>> messageListeners) {
        this.messageListeners = messageListeners;
    }

    public FetchState getFetchState() {
        return fetchState;
    }

    public void setFetchState(FetchState fetchState) {
        this.fetchState = fetchState;
    }

    public String getFetchStateFile() {
        return fetchStateFile;
    }

    public void setFetchStateFile(String fetchStateFile) {
        this.fetchStateFile = fetchStateFile;
    }
}
