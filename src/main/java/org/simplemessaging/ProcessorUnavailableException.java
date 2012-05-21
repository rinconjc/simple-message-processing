/**
 * 
 */
package org.simplemessaging;

/**
 * If the {@link MessageProcessor} for the current {@link MessageListener} is not avaliable then this
 * exception will be called.
 * @author RinconJ
 *
 */
public class ProcessorUnavailableException extends Exception {
	private static final long serialVersionUID = -1259473363110106640L;

	public ProcessorUnavailableException() {
	}

	public ProcessorUnavailableException(String message) {
		super(message);
	}

	public ProcessorUnavailableException(Throwable cause) {
		super(cause);
	}

	public ProcessorUnavailableException(String message, Throwable cause) {
		super(message, cause);
	}
}
