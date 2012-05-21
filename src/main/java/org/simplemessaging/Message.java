package org.simplemessaging;

import java.util.List;

/**
 * Interface that all notifications should implement. A notification contains elements that need to be updated in some way
 * by a processor. A single notification can contain many items that need to be updated and therefore the isEmpty and partition
 * functions are required to see what needs to be updated.
 * 
 * @author RinconJ
 *
 */
public interface Message {

	/**
	 * Partition this notification into a list of smaller notifications.
	 * @param <T>
	 * @param size
	 * @return
	 */
	<T extends Message> List<T> partition(int size);

    /**
     * Number of notification parts contained or max number of partitions of size 1
     * @return
     */
    int size();
	
	/**
	 * Indicates if the notification is empty
	 * @return
	 */
	boolean isEmpty();

    /**
     * Unique identifier of this notification
     * @return
     */
    int getId();

    /**
     * Number of times this notification failed processing
     * @return
     */
    int getFailCount();

    /**
     * Sets the fail count of this notification
     * @param failCount
     */
    void setFailCount(int failCount);
	
}
