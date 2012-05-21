package org.simplemessaging.basic;

import org.apache.log4j.Logger;
import org.simplemessaging.*;
import org.simplemessaging.util.AbstractSmartLifeCycle;
import org.springframework.core.task.TaskExecutor;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Generic implementation of notification listener that supports concurrent processing.
 * @author RinconJ
 *
 * @param <T>
 */
public class GenericMessageListener<T extends Message> extends AbstractSmartLifeCycle implements MessageListener<T> {
	final static Logger logger = Logger.getLogger(GenericMessageListener.class);
	
    protected Executor taskExecutor;

	protected int processingBatchSize = 5;

	protected MessageProcessor<T> messageProcessor;
	
	protected MessageLogger<T> messageLogger;

    protected String listenerId;

    protected int maxRetry=0; //max # of retries before splitting composite notification. 0 means unlimited

    protected int maxRetrySingle=0; //max retries of unitary notifications before marking as invalid. 0 means unlimited

	/**
	 * Receives a message and submits to the processor
	 * @param message
	 * @throws ListenerNotRunningException 
	 * @throws ProcessorUnavailableException 
	 */
	public void receive(T message) throws ListenerNotRunningException, ProcessorUnavailableException{
        if(!this.isRunning() || ! this.isEnabled()){
            throw new ListenerNotRunningException("This listener has not started yet or it has been disabled.");
        }
        if(!messageProcessor.isAvailable()){
        	throw new ProcessorUnavailableException("The message processor " + messageProcessor + " is unavailable.");
        }
        List<T> parts = message.partition(processingBatchSize);
		for(final T part: parts){
			
			messageLogger.logProcessing(listenerId, part);
			
        	taskExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						logger.debug("Processing message:" + part);
						messageProcessor.process(part);
						messageLogger.logCompleted(listenerId, part);
					} catch (FailedProcessingException e) {
						logger.error("Failed processing message:" + part,  e);
						messageLogger.logFailed(listenerId, part, e);
					} catch (Exception e){
						logger.error("Failed processing message:" + part,  e);
						messageLogger.logInvalid(listenerId, part, e);
					}
				}
			});
        }
	}

    /**
     * Resubmits failed messages for reprocessing (this may method may be called periodically by some external scheduling service)
     */
    public void retryFailedMessages(){
        if(!this.isRunning() || ! this.isEnabled()) return;
        List<T> failedMessages = messageLogger.getFailedMessages(listenerId, 20, maxRetry);
        if(!failedMessages.isEmpty()){
            logger.info("Reprocessing failed notifications:" + failedMessages.size());
            try {
                for(T message: failedMessages){
                    if(maxRetrySingle>0 && message.size()==1 && message.getFailCount()>maxRetrySingle){
                        logger.info("Too many failed processing attempts  for message:" + message + ". Marking as invalid.");
                        messageLogger.logInvalid(listenerId, message, new RetryExhaustionException("Failed too many attempts :" + message.getFailCount()));
                    }else if(maxRetry>0 && message.size()>1 && message.getFailCount()>maxRetry){//partition it
                        logger.info("Too many failed processing attempts  for message: " + message + ". Splitting into unitary notifications");
                        messageLogger.logInvalid(listenerId, message, new RetryExhaustionException("Failed too many attempts :" + message.getFailCount()));
                        for(Message part: message.partition(1)){
                            receive((T)part);
                        }
                    } else {
                        receive(message);
                    }
                }
            } catch (Exception e) {
                logger.error("Failed submitting notifications for reprocessing", e);
            }
        }
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE-1;
    }

    @Override
    public void doStart() {
        messageLogger.register(listenerId);
    }

    public Executor getTaskExecutor() {
        return taskExecutor;
    }

    public void setTaskExecutor(Executor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public int getProcessingBatchSize() {
        return processingBatchSize;
    }

    public void setProcessingBatchSize(int processingBatchSize) {
        this.processingBatchSize = processingBatchSize;
    }

    public MessageLogger<T> getMessageLogger() {
        return messageLogger;
    }

    public void setMessageLogger(MessageLogger<T> messageLogger) {
        this.messageLogger = messageLogger;
    }

    public MessageProcessor<T> getMessageProcessor() {
        return messageProcessor;
    }

    public void setMessageProcessor(MessageProcessor<T> messageProcessor) {
        this.messageProcessor = messageProcessor;
    }

    public String getListenerId() {
        return listenerId;
    }

    public void setListenerId(String listenerId) {
        this.listenerId = listenerId;
    }

    public int getMaxRetry() {
        return maxRetry;
    }

    public void setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
    }

    public int getMaxRetrySingle() {
        return maxRetrySingle;
    }

    public void setMaxRetrySingle(int maxRetrySingle) {
        this.maxRetrySingle = maxRetrySingle;
    }
}
