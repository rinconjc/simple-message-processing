package org.simplemessaging;

/**
 * User: rinconj
 * Date: 12/8/11 11:05 AM
 */
public abstract class AbstractMessage implements Message {
    private transient int failCount;

    public int getFailCount() {
        return failCount;
    }

    public void setFailCount(int failCount) {
        this.failCount = failCount;
    }
}
