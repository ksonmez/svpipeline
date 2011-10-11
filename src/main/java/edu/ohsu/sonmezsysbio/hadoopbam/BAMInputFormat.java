package edu.ohsu.sonmezsysbio.hadoopbam;

import fi.tkk.ics.hadoop.bam.SAMRecordWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/20/11
 * Time: 2:17 PM
 */
public class BAMInputFormat extends FileInputFormat<LongWritable,SAMRecordWritable> {
    @Override
    public RecordReader<LongWritable, SAMRecordWritable>
        getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {

        final RecordReader<LongWritable,SAMRecordWritable> rr =
                new BAMRecordReader(jobConf, split);
		return rr;
    }
}
