package org.simplemessaging;

/**
 * This is the interface for notication processing. Every processor must be able to announce wether or not it is avaliable
 * and it must also be capable of processing any notification that it is given.
 * @author RinconJ
 *
 * @param <T>
 */
public interface MessageProcessor<T extends Message> {

	void process(T message) throws FailedProcessingException, InvalidMessageException;
	
	boolean isAvailable();
}
