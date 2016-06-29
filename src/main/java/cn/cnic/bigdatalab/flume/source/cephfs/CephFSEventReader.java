package cn.cnic.bigdatalab.flume.source.cephfs;


import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import cn.cnic.bigdatalab.flume.source.cephfs.CephFsSourceConfigrationConstants.ConsumeOrder;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;


public class CephFSEventReader {

    private static final Logger logger = LoggerFactory
            .getLogger(CephFSEventReader.class);

    private final File cephFS;
    private final String completedSuffix;
    private final String inputCharset;
    private final ConsumeOrder consumeOrder;

    /** Instance var to Cache directory listing **/
    private Iterator<File> candidateFileIter = null;
    private int listFilesCount = 0;

    private Optional<FileInfo> currentFile = Optional.absent();

    private CephFSEventReader(File cephFS,
                              String completedSuffix,
                              String inputCharset,
                              ConsumeOrder consumeOrder
    ) throws IOException {
        Preconditions.checkNotNull(cephFS);
        Preconditions.checkNotNull(completedSuffix);
        Preconditions.checkNotNull(consumeOrder);

        // Verify directory exists and is readable/writable
        Preconditions.checkState(cephFS.exists(),
                "Directory does not exist: " + cephFS.getAbsolutePath());
        Preconditions.checkState(cephFS.isDirectory(),
                "Path is not a directory: " + cephFS.getAbsolutePath());

        // Do a canary test to make sure we have access to spooling directory
        try {
            File canary = File.createTempFile("flume-cephfs-perm-check-", ".canary",
                    cephFS);
            Files.write("testing flume file permissions\n", canary, Charsets.UTF_8);
            List<String> lines = Files.readLines(canary, Charsets.UTF_8);
            Preconditions.checkState(!lines.isEmpty(), "Empty canary file %s", canary);
            if (!canary.delete()) {
                throw new IOException("Unable to delete canary file " + canary);
            }
            logger.debug("Successfully created and deleted canary file: {}", canary);
        } catch (IOException e) {
            throw new FlumeException("Unable to read and modify files" +
                    " in the ceph filesystem: " + cephFS, e);
        }

        this.cephFS = cephFS;
        this.completedSuffix = completedSuffix;
        this.inputCharset = inputCharset;
        this.consumeOrder = consumeOrder;

    }

    /**
     * prase FileInfo into Event
     * @return Event
     * @throws IOException
     */
    public Event readEvent() throws IOException {
        if(!currentFile.isPresent()) {
            currentFile = getNextFile();
        }

        StringBuilder sb = new StringBuilder();
        sb.append(currentFile.get().getFileName());
        sb.append(currentFile.get().getFilePath());
        sb.append(currentFile.get().getLastModified());
        sb.append(currentFile.get().getLength());

        Event event = EventBuilder.withBody(sb.toString(), Charset.forName(inputCharset));

        rollCurrentFile();

        return event;
    }

    /**
     * roll to the next file which is a new arrival
     * @return Optional<FileInfo>
     */
    private Optional<FileInfo> getNextFile() {
        List<File> candidateFiles = Collections.emptyList();

        if(consumeOrder != ConsumeOrder.RANDOM ||
                candidateFileIter == null ||
                !candidateFileIter.hasNext()) {
            FileFilter filter = new FileFilter() {
                public boolean accept(File candidate) {
                    String fileName = candidate.getName();
                    if ((candidate.isDirectory()) ||
                            (fileName.endsWith(completedSuffix)) ||
                            (fileName.startsWith("."))) {
                        return false;
                    }
                    return true;
                }
            };
            candidateFiles = Arrays.asList(cephFS.listFiles(filter));
            listFilesCount++;
            candidateFileIter = candidateFiles.iterator();
        }

        if (!candidateFileIter.hasNext()) {
            return Optional.absent();
        }

        File selectedFile = candidateFileIter.next();
        if(consumeOrder == ConsumeOrder.RANDOM) {
            return Optional.of(new FileInfo(selectedFile));
        }
        else if(consumeOrder == ConsumeOrder.YOUNGEST) {
            for (File candidateFile: candidateFiles) {
                long compare = selectedFile.lastModified() -
                        candidateFile.lastModified();
                if (compare == 0) { // ts is same pick smallest lexicographically.
                    selectedFile = smallerLexicographical(selectedFile, candidateFile);
                } else if (compare < 0) { // candidate is younger (cand-ts > selec-ts)
                    selectedFile = candidateFile;
                }
            }
        }
        else {
            for (File candidateFile: candidateFiles) {
                long compare = selectedFile.lastModified() -
                        candidateFile.lastModified();
                if (compare == 0) { // ts is same pick smallest lexicographically.
                    selectedFile = smallerLexicographical(selectedFile, candidateFile);
                } else if (compare > 0) { // candidate is older (cand-ts < selec-ts).
                    selectedFile = candidateFile;
                }
            }
        }
        return Optional.of(new FileInfo(selectedFile));
    }


    private File smallerLexicographical(File f1, File f2) {
        if (f1.getName().compareTo(f2.getName()) < 0) {
            return f1;
        }
        return f2;
    }

    /**
     * Rename the given ceph file
     * @throws IOException
     */
    private void rollCurrentFile() throws IOException {
        File fileToRoll = new File(currentFile.get().getFilePath());

        this.close();

        if (fileToRoll.lastModified() != currentFile.get().getLastModified()) {
            String message = "File has been modified since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        }
        if (fileToRoll.length() != currentFile.get().getLength()) {
            String message = "File has changed size since being read: " + fileToRoll;
            throw new IllegalStateException(message);
        }

        File dest = new File(fileToRoll.getPath() + completedSuffix);
        logger.info("Preparing to move file {} to {}", fileToRoll, dest);

        // Before renaming, check whether destination file name exists
        if (dest.exists()) {
            String message = "File name has been re-used with different" +
                    " files. Spooling assumptions violated for " + dest;
            throw new IllegalStateException(message);

            // Destination file does not already exist. We are good to go!
        } else {
            boolean renamed = fileToRoll.renameTo(dest);
            if (renamed) {
                logger.debug("Successfully rolled file {} to {}", fileToRoll, dest);
            } else {
            /* If we are here then the file cannot be renamed for a reason other
            * * than that the destination file exists (actually, that remains
            * * possible w/ small probability due to TOC-TOU conditions).*/
                String message = "Unable to move " + fileToRoll + " to " + dest +
                        ". This will likely cause duplicate events. Please verify that " +
                        "flume has sufficient permissions to perform these operations.";
                throw new FlumeException(message);
            }
        }
    }

    /** An immutable class with information about a file being processed. */
    private static class FileInfo {
        private final String fileName;
        private final String filePath;
        private final long length;
        private final long lastModified;

        public FileInfo(File file) {
            this.fileName = file.getName();
            this.filePath = file.getAbsolutePath();
            this.length = file.length();
            this.lastModified = file.lastModified();
        }

        public String getFileName() {
            return fileName;
        }
        public String getFilePath() {
            return filePath;
        }
        public long getLength() {
            return length;
        }
        public long getLastModified() {
            return lastModified;
        }

    }

    public static class Builder {
        private File cephFS;
        private String completedSuffix =
                CephFsSourceConfigrationConstants.CEPH_FILE_SUFFIX;
        private String ignorePattern =
                CephFsSourceConfigrationConstants.DEFAULT_IGNORE_PAT;
        private String inputCharset =
                CephFsSourceConfigrationConstants.DEFAULT_INPUT_CHARSET;
        private ConsumeOrder consumeOrder =
                CephFsSourceConfigrationConstants.DEFAULT_CONSUME_ORDER;

        public Builder cephFS(File directory) {
            this.cephFS = directory;
            return this;
        }

        public Builder completedSuffix(String completedSuffix) {
            this.completedSuffix = completedSuffix;
            return this;
        }

        public Builder ignorePattern(String ignorePattern) {
            this.ignorePattern = ignorePattern;
            return this;
        }

        public Builder inputCharset(String inputCharset) {
            this.inputCharset = inputCharset;
            return this;
        }

        public Builder consumeOrder(ConsumeOrder consumeOrder) {
            this.consumeOrder = consumeOrder;
            return this;
        }

        public CephFSEventReader build() throws IOException {
            return new CephFSEventReader(
                    cephFS, completedSuffix, inputCharset, consumeOrder);
        }
    }


    public void close() throws IOException {
        if (currentFile.isPresent()) {
            currentFile = Optional.absent();
        }
    }
}
