package org.simplemessaging;

import java.util.List;

/**
 * This is the logging interface for the events that must occur in a {@link org.simplemessaging.fetching.MessageFetcher} or a {@link MessageListener}.
 * @author MassaioliR
 *
 * @param <T>
 */
public interface MessageLogger<T extends Message> {

    void register(String listenerName);
	
	void logProcessing(String listenerId, T notification);
	
	void logCompleted(String listenerId, T notification);
	
	void logFailed(String listenerId, T notification, Exception e);
	
	void logInvalid(String listenerId, T notification, Exception e);

    List<T> getInProcessMessages(String listenerId, int max);

    List<T> getFailedMessages(String listenerId, int max);
    
    List<T> getFailedMessages(String listenerName, int max, int maxFailCount);
}
