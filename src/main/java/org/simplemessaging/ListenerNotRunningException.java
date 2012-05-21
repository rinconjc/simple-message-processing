/**
 * 
 */
package org.simplemessaging;

/**
 * If a {@link MessageListener} is not currently running the listening thread then the recieve() function will call this error.
 * @author RinconJ
 *
 */
public class ListenerNotRunningException extends Exception {
	private static final long serialVersionUID = 4398213988356714908L;

	public ListenerNotRunningException() {
	}

	public ListenerNotRunningException(String message) {
		super(message);
	}

	public ListenerNotRunningException(Throwable cause) {
		super(cause);
	}

	public ListenerNotRunningException(String message, Throwable cause) {
		super(message, cause);
	}
}
