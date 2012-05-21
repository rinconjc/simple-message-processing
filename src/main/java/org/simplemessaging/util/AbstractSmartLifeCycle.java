package org.simplemessaging.util;

import org.springframework.context.SmartLifecycle;

/**
 * Created by IntelliJ IDEA.
 * User: RinconJ
 * Date: 12/05/11
 * Time: 3:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class AbstractSmartLifeCycle implements SmartLifecycle{

    private boolean running=false;
    private boolean enabled=true;

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable runnable) {
        running = false;
        runnable.run();
    }

    @Override
    public void start() {
        if(!enabled) return;
        doStart();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running && !Thread.currentThread().isInterrupted();
    }

    @Override
    public int getPhase() {
        return 0;
    }

    protected void doStart(){

    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
