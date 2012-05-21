package org.simplemessaging;

/**
 * This is thrown when a {@link MessageProcessor} fails to correctly process a notification. There are many different causes
 * that could arise that would cause this error to be thrown and the scenarios in which it is thrown depends on the instance of 
 * the notification processor.
 * @author MassaioliR
 *
 */
public class FailedProcessingException extends Exception {
	private static final long serialVersionUID = 1L;

	public FailedProcessingException() {
	}

	public FailedProcessingException(String message) {
		super(message);
	}

	public FailedProcessingException(Throwable cause) {
		super(cause);
	}

	public FailedProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

}
