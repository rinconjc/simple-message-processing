package org.simplemessaging;

/**
 * This exception is called when a {@link MessageProcessor} looks into a notification and finds that something in the notification
 * is illegal or incorrect. Like the source of the notification for example.
 * @author MassaioliR
 *
 */
public class InvalidMessageException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InvalidMessageException() {
	}

	public InvalidMessageException(String message) {
		super(message);
	}

	public InvalidMessageException(Throwable cause) {
		super(cause);
	}

	public InvalidMessageException(String message, Throwable cause) {
		super(message, cause);
	}
}
