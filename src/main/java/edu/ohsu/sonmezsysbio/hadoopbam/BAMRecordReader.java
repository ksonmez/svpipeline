package edu.ohsu.sonmezsysbio.hadoopbam;

import fi.tkk.ics.hadoop.bam.FileVirtualSplit;
import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import fi.tkk.ics.hadoop.bam.custom.samtools.BAMRecordCodec;
import fi.tkk.ics.hadoop.bam.custom.samtools.SAMFileReader;
import fi.tkk.ics.hadoop.bam.util.WrapSeekable;
import net.sf.samtools.util.BlockCompressedInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/20/11
 * Time: 2:21 PM
 */
public class BAMRecordReader implements RecordReader<LongWritable,SAMRecordWritable> {

    private final LongWritable key = new LongWritable();
    private final SAMRecordWritable record = new SAMRecordWritable();

    private BlockCompressedInputStream bci;
    private BAMRecordCodec codec;
    private long fileStart, virtualEnd;
    private InputSplit split;

    public BAMRecordReader(JobConf jobConf, InputSplit split) throws IOException {
        this.split = split;

//        final Path file = split.getPath();
//        final FileSystem fs = FileSystem.get(jobConf);
//
//        final FSDataInputStream in = fs.open(file);
//        codec = new BAMRecordCodec(new SAMFileReader(in).getFileHeader());
//
//        in.seek(0);
//        bci =
//            new BlockCompressedInputStream(
//                new WrapSeekable<FSDataInputStream>(
//                    in, fs.getFileStatus(file).getLen(), file));
//
//        final long virtualStart = split.getStartVirtualOffset();
//
//        fileStart  = virtualStart >>> 16;
//        virtualEnd = split.getEndVirtualOffset();
//
//        bci.seek(virtualStart);
//        codec.setInputStream(bci);

    }

    public boolean next(LongWritable key, SAMRecordWritable value) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public LongWritable createKey() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public SAMRecordWritable createValue() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public long getPos() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public float getProgress() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
