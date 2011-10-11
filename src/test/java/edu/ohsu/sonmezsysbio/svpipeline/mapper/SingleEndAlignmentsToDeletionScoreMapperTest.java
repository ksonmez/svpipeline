package edu.ohsu.sonmezsysbio.svpipeline.mapper;

import edu.ohsu.sonmezsysbio.svpipeline.mapper.SingleEndAlignmentsToDeletionScoreMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 6/5/11
 * Time: 9:10 PM
 */
public class SingleEndAlignmentsToDeletionScoreMapperTest {

    @Test
    public void testMap() throws Exception {
        //String inputLine = "@SimSeq_100064\t@SimSeq_100064/1\tS\tGGAATGCTGAACCGAAGCCACACATATATTACATTGCTGATATTTCCATG\t22335345345444554445454566655554555335344444432221\tU\t159\t40\t>2\t41823857\tF\t.\t.\t.\t6A>G 13A>C 15T>A 19T>C 36A>G 42T>A 45A>T\tSEP\t@SimSeq_100064/2\tS\tATATCCGTATGTCAGATTTTATAATCTAGATATTTGATGGCTTTTTTTTA\t43555556665767677777777876D76777676566554665655543\tU\t79\t126\t>2\t41821123\tR\t.\t.\t.\t20T>A 22A>C 27C>T";
        String inputLine = "@SimSeq_100064\t@SimSeq_100064/1\tS\tGGAATGCTGAACCGAAGCCACACATATATTACATTGCTGATATTTCCATG\t22335345345444554445454566655554555335344444432221\tU\t159\t40\t>2\t41823857\tF\t.\t.\t.\t6A>G 13A>C 15T>A 19T>C 36A>G 42T>A 45A>T\tSEP\t@SimSeq_100064/2\tS\tATATCCGTATGTCAGATTTTATAATCTAGATATTTGATGGCTTTTTTTTA\t43555556665767677777777876D76777676566554665655543\tU\t79\t126\t>2\t41821123\tR\t.\t.\t.\t20T>A 22A>C 27C>T";

        SingleEndAlignmentsToDeletionScoreMapper mapper = new SingleEndAlignmentsToDeletionScoreMapper();
        mapper.setTargetIsize(3000.0);
        mapper.setTargetIsizeSD(300.0);

        OutputCollector<Text, DoubleWritable> collector = Mockito.mock(OutputCollector.class);
        Reporter reporter = Mockito.mock(Reporter.class);

        mapper.map(new LongWritable(1), new Text(inputLine), collector, reporter);

        for (int i = 41821100; i <= 41823900; i = i + 50) {
            verify(collector).collect(new Text(">2\t" + i),
                    new DoubleWritable(-0.9998999999997488));
        }

        verifyNoMoreInteractions(collector);


    }

    @Test
    public void testComputeDeletionScore() throws Exception {
        // bigger insert size: more likely deletion
        assertTrue(SingleEndAlignmentsToDeletionScoreMapper.computeDeletionScore(185, 117, 5114, 3000.0, 300.0) >
                   SingleEndAlignmentsToDeletionScoreMapper.computeDeletionScore(185, 117, 3114, 3000.0, 300.0));
        // higher quality: more likely deletion
        assertTrue(SingleEndAlignmentsToDeletionScoreMapper.computeDeletionScore(185, 117, 3114, 3000.0, 300.0) <
                   SingleEndAlignmentsToDeletionScoreMapper.computeDeletionScore(185, 20, 3114, 3000.0, 300.0));
    }

}
