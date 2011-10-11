package edu.ohsu.sonmezsysbio.hadoopbam;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/21/11
 * Time: 5:21 PM
 */
public class FileVirtualSplit implements InputSplit {

    private Path file;
    private long vStart;
    private long vEnd;
    private final String[] locations;

    public FileVirtualSplit() {
        locations = null;
    }

    public FileVirtualSplit(Path f, long vs, long ve, String[] locs) {
        file      = f;
        vStart    = vs;
        vEnd      = ve;
        locations = locs;
    }

    public long getLength() throws IOException {
        return 0;
    }

    public String[] getLocations() throws IOException {
        return locations;
    }

    public void write(DataOutput out) throws IOException {

    }

    public void readFields(DataInput in) throws IOException {

    }
}
