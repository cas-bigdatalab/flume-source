# Flume Directory Source

A flume source for detecting and collecting the metadata of the new arrival files(not only for ceph)

## Requirements
This library requires JDK 1.7+

## Linking
You can link against this library in your program at the following coordinates:

```
groupId: cn.cnic.bigdatalab
artifactId: flume-ceph-source
version: 1.0.0
```

## Features
This library allows detecting the new arrival files in specific directory you config,
and sending the metadata to flume's channel

When using ceph as file system, you can detect new arrival files in directory which ceph file system mount on.

The library accepts several options:
* `cephFS`: The directory from which to read files from.
* `fileSuffix`: Suffix to append to completely ingested files.
* `ignorePattern`: Regular expression specifying which files to ignore.
* `consumeOrder`: In which order files in the directory will be consumed oldest, youngest and random.
In case of oldest and youngest, the last modified time of the files will be used to compare the files.
In case of a tie, the file with smallest laxicographical order will be consumed first.
In case of random any file will be picked randomly.
When using oldest and youngest the whole directory will be scanned to pick the oldest/youngest file, which might be slow if there are a large number of files, while using random may cause old files to be consumed very late if new files keep coming in the directory.
