/**
 * 
 */
package org.simplemessaging;

/**
 * A Message Listener accepts new updates through a Push based system. It accepts new notifications and processes them.
 * @author RinconJ
 *
 */
public interface MessageListener<T extends Message> {
	
	/**
	 * @param notification
	 * @throws ListenerNotRunningException
	 * @throws ProcessorUnavailableException
	 */
	void receive(T notification) throws ListenerNotRunningException, ProcessorUnavailableException; 

}
