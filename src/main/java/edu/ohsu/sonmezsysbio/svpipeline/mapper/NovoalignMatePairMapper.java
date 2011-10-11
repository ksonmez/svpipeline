package edu.ohsu.sonmezsysbio.svpipeline.mapper;

import edu.ohsu.sonmezsysbio.svpipeline.SAMRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/21/11
 * Time: 5:36 PM
 */
public class NovoalignMatePairMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private OutputCollector<Text, Text> output;
    private String localDir;
    private Writer s1FileWriter;
    private Writer s2FileWriter;
    private File s1File;
    private File s2File;
    private String reference;
    private String repeatReport;
    private String targetIsize;
    private String targetIsizeSD;
    private String libraryName;
    private Reporter reporter;
    private static boolean done = false;

    public OutputCollector<Text, Text> getOutput() {
        return output;
    }

    public void setOutput(OutputCollector<Text, Text> output) {
        this.output = output;
    }

    public Reporter getReporter() {
        return reporter;
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);

        System.err.println("Current dir: " + new File(".").getAbsolutePath());

        this.localDir = job.get("mapred.child.tmp");
        try {
            s1File = new File(localDir + "/temp1_sequence.txt").getAbsoluteFile();
            s2File = new File(localDir + "/temp2_sequence.txt").getAbsoluteFile();
            s1File.createNewFile();
            s2File.createNewFile();
            s1FileWriter = new BufferedWriter(new FileWriter(s1File));
            s2FileWriter = new BufferedWriter(new FileWriter(s2File));

            reference = job.get("novoalign.reference");
            repeatReport = job.get("novoalign.repeatReport");
            targetIsize = job.get("novoalign.targetIsize");
            targetIsizeSD = job.get("novoalign.targetIsizeSD");
            libraryName = job.get("novoalign.libraryName");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (this.output == null) {
            this.output = output;
        }
        if (this.reporter == null) {
            this.reporter = reporter;
        }

        String line = value.toString();
        String[] fields = line.split("\t");

        s1FileWriter.write(fields[0] + "\n");
        s1FileWriter.write(fields[1] + "\n");
        s1FileWriter.write(fields[2] + "\n");
        s1FileWriter.write(fields[3] + "\n");

        s2FileWriter.write(fields[4] + "\n");
        s2FileWriter.write(fields[5] + "\n");
        s2FileWriter.write(fields[6] + "\n");
        s2FileWriter.write(fields[7] + "\n");

        reporter.progress();
        //System.out.println("Done with map method, real work will happen in map");
    }

    class ProgressReporter implements Runnable {
        public void run() {
            while (! done) {
                try {
                    Thread.sleep(10000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                reporter.progress();
            }

        }
    }

    @Override
    public void close() throws IOException {
        super.close();

        s1FileWriter.close();
        s2FileWriter.close();

        //Thread progressThread = new Thread(new ProgressReporter());
        //progressThread.start();
        String[] commandLine = buildCommandLine(reference, s1File.getPath(), s2File.getPath(), targetIsize, targetIsizeSD, repeatReport, libraryName);
        System.err.println("Executing command: " + Arrays.toString(commandLine));
        Process p = Runtime.getRuntime().exec(commandLine);


        BufferedReader stdInput = new BufferedReader(new
                         InputStreamReader(p.getInputStream()));

        readAlignments(stdInput, p.getErrorStream());
        done = true;

    }

    protected void readAlignments(BufferedReader stdInput, InputStream errorStream) throws IOException {
        String outLine;
        int numPairs = 0;
        String lastReadPairId = null;
        List<String> read1Lines = new ArrayList<String>();
        List<String> read2Lines = new ArrayList<String>();
        boolean properPairs = false;
        while ((outLine = stdInput.readLine()) != null) {
            //System.err.println("LINE: " + outLine);
            if (outLine.startsWith("@")) continue;
            if (outLine.startsWith("Novoalign") || outLine.startsWith("Exception")) {
                String error = printErrorStream(errorStream);
                throw new RuntimeException(error);
            }

            String readPairId = outLine.substring(0,outLine.indexOf('\t'));
            SAMRecord samRecord = SAMRecord.parseSamRecord(outLine.split("\t"));

            if (! samRecord.isMapped()) {
                continue;
            }

            if (!readPairId.equals(lastReadPairId)) {
                if (! properPairs) {
                    numPairs = emitAllPairs(lastReadPairId, numPairs, read1Lines, read2Lines);
                    read1Lines.clear();
                    read2Lines.clear();
                }

                lastReadPairId = readPairId;
                properPairs = samRecord.isProperPair();
            }

            if (samRecord.isAlignmentOfFirstRead()) {
                read1Lines.add(outLine);
            } else {
                read2Lines.add(outLine);
            }

            if (properPairs && read1Lines.size() == 1 && read2Lines.size() == 1) {
                numPairs = emitAllPairs(readPairId, numPairs, read1Lines, read2Lines);
                read1Lines.clear();
                read2Lines.clear();
            }

        }
        numPairs = emitAllPairs(lastReadPairId, numPairs, read1Lines, read2Lines);
        read1Lines.clear();
        read2Lines.clear();

    }

    private int emitAllPairs(String readPairId, int numPairs, List<String> read1Lines, List<String> read2Lines) throws IOException {
        int newNumPairs = numPairs;
        for (int i = 0; i < read1Lines.size(); i++) {
            for (int j = 0; j < read2Lines.size(); j++) {
                String value = read1Lines.get(i) + "\tSEP\t" + read2Lines.get(j);
                String key = readPairId + "-" + newNumPairs;
                output.collect(new Text(key), new Text(value));
                newNumPairs = newNumPairs+1;
            }
        }
        return newNumPairs;
    }

    private String printErrorStream(InputStream errorStream) throws IOException {
        String outLine;BufferedReader stdErr = new BufferedReader(new
                InputStreamReader(errorStream));
        String firstErrorLine = null;
        while ((outLine = stdErr.readLine()) != null) {
            if (firstErrorLine == null) firstErrorLine = outLine;
            System.err.println(outLine);
        }
        return firstErrorLine;
    }

    protected static String[] buildCommandLine(String reference, String path1, String path2, String targetIsize,
                                               String targetIsizeSD, String repeatReport, String libraryName) {
        String[] commandArray = {
                "/g/whelanch/software/bin/" + "novoalign",
                "-d", reference,
                "-c", "1",
                "-f", path1, path2,
                "-F", "ILMFQ",
                "-k", "-K", "calfile.txt",
                "-i", "MP", targetIsize + "," + targetIsizeSD, //"150,50",
                "-r", "Ex", "10", "-t", "350",
                //"-a", "GATCGGAAGAGCGGTTCAGCA", "GATCGGAAGAGCGTCGTGTAGGGA",
                "-oSAM", "@RG\\tID:RGID\\tPU:ILLUMINA\\tLB:" + libraryName + "\\tSM:" + libraryName
        };
//        String args = String.format("-d %s -c 1 -f %s %s -F ILMFQ -k -K calfile.txt -i MP %s,%s 150,50" +
//                " -a GATCGGAAGAGCGGTTCAGCA GATCGGAAGAGCGTCGTGTAGGGA " +
//                " -r %s -oSAM \"@RG\tID:RGID\tPU:ILLUMINA\tLB:%s\tSM:%s\" ",
//                reference, path1, path2, targetIsize, targetIsizeSD, repeatReport, libraryName, libraryName); // todo: read group ID?
//
//        return "/g/whelanch/software/bin/" + "novoalign " + args; //todo: unhardcode path
        return commandArray;
    }
}
