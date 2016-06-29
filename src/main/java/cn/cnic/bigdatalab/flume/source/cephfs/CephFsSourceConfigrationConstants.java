package cn.cnic.bigdatalab.flume.source.cephfs;


public class CephFsSourceConfigrationConstants {

    /** Directory where files are deposited. */
    public static final String CEPH_FILESYSTEM = "cephFS";

    /** Suffix appended to files when they are finished being sent. */
    public static final String CEPH_FILE_SUFFIX = "fileSuffix";
    public static final String DEFAULT_CEPH_FILE_SUFFIX = ".COMPLETED";

    /** Pattern of files to ignore */
    public static final String IGNORE_PAT = "ignorePattern";
    public static final String DEFAULT_IGNORE_PAT = "^$"; // no effect

    /** Character set used when reading the input. */
    public static final String INPUT_CHARSET = "inputCharset";
    public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

    /**  */
    public static final String MAX_BACKOFF = "maxBackoff";
    public static final Integer DEFAULT_MAX_BACKOFF = 4000;

    /** Consume order. */
    public enum ConsumeOrder {
        OLDEST, YOUNGEST, RANDOM
    }
    public static final String CONSUME_ORDER = "consumeOrder";
    public static final ConsumeOrder DEFAULT_CONSUME_ORDER = ConsumeOrder.OLDEST;

}
