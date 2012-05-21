package org.simplemessaging;

/**
 * User: rinconj
 * Date: 12/8/11 12:11 PM
 */
public class RetryExhaustionException extends Exception {
    public RetryExhaustionException() {
    }

    public RetryExhaustionException(String message) {
        super(message);
    }

    public RetryExhaustionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryExhaustionException(Throwable cause) {
        super(cause);
    }

}
