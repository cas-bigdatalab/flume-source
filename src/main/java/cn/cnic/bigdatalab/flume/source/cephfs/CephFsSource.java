package cn.cnic.bigdatalab.flume.source.cephfs;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static cn.cnic.bigdatalab.flume.source.cephfs.CephFsSourceConfigrationConstants.*;

public class CephFsSource extends AbstractSource implements
        Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory
            .getLogger(CephFsSource.class);

    // Delay used when polling for new files
    private static final int POLL_DELAY_MS = 500;

    /* Config options */
    private String cephDirectory;
    private String completedSuffix;
    private String ignorePattern;
    private String inputCharset;
    CephFSEventReader reader;
    private volatile boolean hasFatalError = false;

    private SourceCounter sourceCounter;
    private boolean backoff = true;
    private boolean hitChannelException = false;
    private int maxBackoff;
    private ScheduledExecutorService executor;
    private ConsumeOrder consumeOrder;

    public synchronized void start() {
        logger.info("CephFsSource source starting with directory: {}",
                cephDirectory);

        executor = Executors.newSingleThreadScheduledExecutor();

        File directory = new File(cephDirectory);

        try {
            reader = new CephFSEventReader.Builder()
                    .cephFS(directory)
                    .completedSuffix(completedSuffix)
                    .ignorePattern(ignorePattern)
                    .inputCharset(inputCharset)
                    .consumeOrder(consumeOrder)
                    .build();
        } catch (IOException e) {
            throw new FlumeException("Error instantiating event parser", e);
        }

        Runnable runner = new CephfsSourceRunnable(reader, sourceCounter);
        executor.scheduleWithFixedDelay(
                runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

        super.start();
        logger.debug("CephFS source started");
        sourceCounter.start();
    }

    public synchronized void stop() {
        executor.shutdown();
        try {
            executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            logger.info("Interrupted while awaiting termination", ex);
        }
        executor.shutdownNow();

        super.stop();
        sourceCounter.stop();
        logger.info("CephFS source {} stopped. Metrics: {}", getName(),
                sourceCounter);
    }

    public synchronized void configure(Context context) {
        cephDirectory = context.getString(CEPH_FILESYSTEM);
        completedSuffix = context.getString(CEPH_FILE_SUFFIX, DEFAULT_CEPH_FILE_SUFFIX);
        ignorePattern = context.getString(IGNORE_PAT, DEFAULT_IGNORE_PAT);
        inputCharset = context.getString(INPUT_CHARSET, DEFAULT_INPUT_CHARSET);
        consumeOrder = ConsumeOrder.valueOf(context.getString(CONSUME_ORDER,
                DEFAULT_CONSUME_ORDER.toString()).toUpperCase(Locale.ENGLISH));
        maxBackoff = context.getInteger(MAX_BACKOFF, DEFAULT_MAX_BACKOFF);
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @VisibleForTesting
    protected boolean hasFatalError() {
        return hasFatalError;
    }

    /**
     * The class always backs off, this exists only so that we can test without
     * taking a really long time.
     * @param backoff - whether the source should backoff if the channel is full
     */
    @VisibleForTesting
    protected void setBackOff(boolean backoff) {
        this.backoff = backoff;
    }

    @VisibleForTesting
    protected boolean hitChannelException() {
        return hitChannelException;
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    private class CephfsSourceRunnable implements Runnable {
        private CephFSEventReader reader;
        private SourceCounter sourceCounter;

        public CephfsSourceRunnable(CephFSEventReader reader,
                                    SourceCounter sourceCounter) {
            this.reader = reader;
            this.sourceCounter = sourceCounter;
        }

        public void run() {
            int backoffInterval = 250;
            try {
                while (!Thread.interrupted()) {
                    Event event = reader.readEvent();

                    sourceCounter.incrementAppendAcceptedCount();
                    sourceCounter.incrementEventAcceptedCount();

                    try {
                        getChannelProcessor().processEvent(event);
                    } catch (ChannelException ex) {
                        logger.warn("The channel is full, and cannot write data now. The " +
                                "source will try again after " + String.valueOf(backoffInterval) +
                                " milliseconds");
                        if (backoff) {
                            TimeUnit.MILLISECONDS.sleep(backoffInterval);
                            backoffInterval = backoffInterval << 1;
                            backoffInterval = backoffInterval >= maxBackoff ? maxBackoff :
                                    backoffInterval;
                        }
                        continue;
                    }
                    backoffInterval = 250;
                    sourceCounter.incrementAppendAcceptedCount();
                    sourceCounter.incrementEventAcceptedCount();
                }
            } catch (Throwable t) {
                logger.error("FATAL: " + CephFsSource.this.toString() + ": " +
                        "Uncaught exception in CephFsSource thread. " +
                        "Restart or reconfigure Flume to continue processing.", t);
                hasFatalError = true;
                Throwables.propagate(t);
            }

        }
    }
}
