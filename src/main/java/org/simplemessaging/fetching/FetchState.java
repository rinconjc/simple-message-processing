package org.simplemessaging.fetching;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 * Represents a generic state of a fetching process. Eventually you will need to fetch data from a CMS and when you do 
 * you tend to get that data in chunks and at a certain time. The FetchState object allows you to keep track of the level
 * of completion of each fetch.
 * @author RinconJ
 *
 */
public class FetchState implements Cloneable{
	// Bit of a hack so that we can persist correctly
	private static final String INT_SUFFIX = "(int)";
	private static final String TIME_SUFFIX = "(time)";
	
	// Is the Fetch of new data still going or has it ended?
	public enum Status{
		COMPLETED,
		CONTINUE
	}
	
	private Status status;
	
	private Map<String, Object> params = new HashMap<String, Object>();

    public FetchState() {
    }

    public FetchState(Status status, Map<String, Object> params) {
        this.status = status;
        this.params = params;
    }

    public FetchState copy(){
		return new FetchState(status, new HashMap<String, Object>(params));
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getParam(Object key) {
		return (T) params.get(key);
	}

	public void putParam(String key, Object value) {
		params.put(key, value);
	}
	
	/**
	 * This function allows you to persist the state of the fetch so that, in the event that the fetching process crashes, you can
	 * start the fetching right back up again and it will continue like nothing went wrong. This is the loading half of the equation.
	 * @param filename
	 * @return
	 * @throws Exception
	 */
	public static FetchState loadFrom(String filename) throws Exception{
		FetchState fetchState = new FetchState();
		fetchState.setStatus(Status.CONTINUE);
		
        Properties props = new Properties();

        File file = new File(filename);
        if(!file.exists()){
            return null;
        }
        props.load(new FileInputStream(file));
        for (Entry<Object,Object> entry : props.entrySet()) {
        	parseProperty(fetchState, (String)entry.getKey(), (String)entry.getValue());
		}
        return fetchState;
    }

	/**
	 * This function allows you to persist the state of the fetch so that, in the event that the fetching process crashes, you can
	 * start the fetching right back up again and it will continue like nothing went wrong. This is the saving half of the equation.
	 * @param filename
	 * @throws Exception
	 */
    public void saveTo(String filename) throws Exception{
        Properties props = new Properties();
        for(Entry<String,Object> entry: params.entrySet()){
        	storeProperty(props, entry.getKey(), entry.getValue());
        }
        props.store(new FileOutputStream(filename), "FetchState properties");
    }	
    
    private static void parseProperty(FetchState fetchState, String key, String value){
		if(key.endsWith(INT_SUFFIX)){
			fetchState.putParam(key.substring(0, key.length()-INT_SUFFIX.length()), parseInt(value));
		}else if(key.endsWith(TIME_SUFFIX)){
			fetchState.putParam(key.substring(0, key.length()-TIME_SUFFIX.length()), new Date(parseLong(value)));
		}else{
			fetchState.putParam(key, value);
		}
    }
    
    private static void storeProperty(Properties props, String key, Object value){
		if(value instanceof Integer){
			props.setProperty(key+INT_SUFFIX, value.toString());
		}else if(value instanceof Date){
			props.setProperty(key+TIME_SUFFIX, String.valueOf(((Date)value).getTime()));
		}else{
			props.setProperty(key, value.toString());
		}
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
