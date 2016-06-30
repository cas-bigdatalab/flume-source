package cn.cnic.bigdatalab.flume.source.cephfs;


import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCephSource {
    static CephFsSource source;
    static MemoryChannel channel;
    private File tmpDir;

    @Before
    public void setUp() {
        source = new CephFsSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
        tmpDir = Files.createTempDir();
    }

    @After
    public void tearDown() {
        for (File f : tmpDir.listFiles()) {
            f.delete();
        }
        tmpDir.delete();
    }

    @Test
    public void testGetFiles() throws IOException {
        Context context = new Context();

        context.put(CephFsSourceConfigrationConstants.CEPH_FILESYSTEM,
                tmpDir.getAbsolutePath());

        File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

        Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                        "file1line5\nfile1line6\nfile1line7\n",
                f1, Charsets.UTF_8);

        Configurables.configure(source, context);
        source.start();

        Transaction txn = channel.getTransaction();
        txn.begin();

        Event event = channel.take();
        Assert.assertNotNull(event);
    }

    @Test
    public void testAddNewFiles() throws IOException {
        Context context = new Context();

        context.put(CephFsSourceConfigrationConstants.CEPH_FILESYSTEM,
                tmpDir.getAbsolutePath());

        File f2 = new File(tmpDir.getAbsolutePath() + "/file2");

        Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                        "file1line5\nfile1line6\n",
                f2, Charsets.UTF_8);

        Configurables.configure(source, context);
        source.start();

        Transaction txn = channel.getTransaction();
        txn.begin();

        Event event = channel.take();
        Assert.assertNotNull(event);
    }

    @Test
    public void testLifecycle() throws IOException, InterruptedException {
        Context context = new Context();

        context.put(CephFsSourceConfigrationConstants.CEPH_FILESYSTEM,
                tmpDir.getAbsolutePath());

        Configurables.configure(source, context);

        for (int i = 0; i < 10; i++) {
            source.start();

            Assert
                    .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
                            source, LifecycleState.START_OR_ERROR));
            Assert.assertEquals("Server is started", LifecycleState.START,
                    source.getLifecycleState());

            source.stop();
            Assert.assertTrue("Reached stop or error",
                    LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
            Assert.assertEquals("Server is stopped", LifecycleState.STOP,
                    source.getLifecycleState());
        }
    }
}
