package edu.ohsu.sonmezsysbio.svpipeline.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 6/5/11
 * Time: 2:34 PM
 */
public class NovoalignSingleEndAlignmentsToPairsReducer extends MapReduceBase
        implements Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterator<Text> values,
                       OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<String> read1Alignments = new ArrayList<String>();
        List<String> read2Alignments = new ArrayList<String>();
        while (values.hasNext()) {
            String alignment = values.next().toString();
            char readNum = alignment.charAt(alignment.indexOf("\t") - 1);
            if (readNum == '1') {
                read1Alignments.add(alignment);
            } else if (readNum == '2') {
                read2Alignments.add(alignment);
            } else {
                throw new RuntimeException("bad line: " + alignment);
            }
        }

        for (String aligment1 : read1Alignments) {
            for (String alignment2 : read2Alignments) {
                output.collect(key, new Text(aligment1 + "\tSEP\t" + alignment2));
            }
        }
    }
}
