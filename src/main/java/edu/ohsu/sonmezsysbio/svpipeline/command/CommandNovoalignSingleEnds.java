package edu.ohsu.sonmezsysbio.svpipeline.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import edu.ohsu.sonmezsysbio.svpipeline.reducer.NovoalignSingleEndAlignmentsToPairsReducer;
import edu.ohsu.sonmezsysbio.svpipeline.mapper.NovoalignSingleEndMapper;
import edu.ohsu.sonmezsysbio.svpipeline.SVPipeline;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.*;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/18/11
 * Time: 2:01 PM
 */
@Parameters(separators = "=", commandDescription = "Run a novoalign mate pair alignment")
public class CommandNovoalignSingleEnds implements SVPipelineCommand {

    @Parameter(names = {"--HDFSDir"}, required = true)
    String hdfsDir;

    @Parameter(names = {"--fastqFile1"}, required = true)
    String readFile1;

    @Parameter(names = {"--fastqFile2"}, required = true)
    String readFile2;

    @Parameter(names = {"--reference"}, required = true)
    String reference;

    private int numRecords;

    public void copyReadFilesToHdfs() throws IOException {
        Configuration config = new Configuration();

        FileSystem hdfs = FileSystem.get(config);

        FSDataOutputStream outputStream = hdfs.create(new Path(hdfsDir + "/" + "novoIn.txt"));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream));

        try {
            readFile(writer, readFile1);
            readFile(writer, readFile2);
        } finally {
            writer.close();
        }

    }

    private void readFile(BufferedWriter writer, String pathname) throws IOException {
        BufferedReader inputReader1 = new BufferedReader(new FileReader(new File(pathname)));

        numRecords = 0;
        try {
            String convertedFastqLine = readFastqEntry(inputReader1);
            while (convertedFastqLine != null) {
                writer.write(convertedFastqLine);
                convertedFastqLine = readFastqEntry(inputReader1);
                numRecords++;
            }
        } finally {
            inputReader1.close();
        }
    }

    private String readFastqEntry(BufferedReader inputReader1) throws IOException {
        String read1 = inputReader1.readLine();
        if (read1 == null) {
            return null;
        }

        String seq1 = inputReader1.readLine();
        String sep1 = inputReader1.readLine();
        String qual1 = inputReader1.readLine();

        StringBuffer lineBuffer = new StringBuffer();
        lineBuffer.append(read1).append("\t").append(seq1).append("\t").append(sep1).append("\t").append(qual1);
        lineBuffer.append("\n");
        return lineBuffer.toString();
    }

    public void runHadoopJob() throws IOException {
        JobConf conf = new JobConf();

        conf.setJobName("Single End Alignment");
        conf.setJarByClass(SVPipeline.class);
        FileInputFormat.addInputPath(conf, new Path(hdfsDir));
        Path outputDir = new Path("/user/whelanch/tmp/se_align_out/");
        FileSystem.get(conf).delete(outputDir);

        FileOutputFormat.setOutputPath(conf, outputDir);

        conf.setInputFormat(TextInputFormat.class);

        conf.set("mapred.task.timeout", "3600000");
        conf.set("novoalign.reference", reference);

        conf.setMapperClass(NovoalignSingleEndMapper.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setNumMapTasks(numRecords / 60000 + 1);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
        conf.setCompressMapOutput(true);

        conf.setReducerClass(NovoalignSingleEndAlignmentsToPairsReducer.class);

        JobClient.runJob(conf);

    }

    public void run() throws IOException {
        copyReadFilesToHdfs();
        runHadoopJob();

    }
}
