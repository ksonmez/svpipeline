package edu.ohsu.sonmezsysbio.svpipeline.mapper;

import edu.ohsu.sonmezsysbio.svpipeline.mapper.NovoalignMatePairMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import static org.mockito.Mockito.*;

/**
 * Created by IntelliJ IDEA.
 * User: cwhelan
 * Date: 5/21/11
 * Time: 10:05 PM
 */
public class NovoalignMatePairMapperTest {

    @Test
    public void testBuildCommandLine() {
//        assertEquals(
//                "novoalign -d ref -c 1 -f path1 path2 -F ILMFQ -k -K calfile.txt -i MP 3000,300 150,50 -a GATCGGAAGAGCGGTTCAGCA GATCGGAAGAGCGTCGTGTAGGGA  -r Random -oSAM $\"@RG\tID:$RG\tPU:ILLUMINA\tLB:TC1\tSM:TC1\"",
//                NovoalignMatePairMapper.buildCommandLine("ref", "path1", "path2", "3000", "300", "Random", "TC1"));
    }

    @Test
    public void testReadAlignments_improperPairs() throws IOException {
        String alignments = "SimSeq_24fc9f\t97\t2\t170483133\t21\t50M\t=\t101837970\t-68645115\tCAGGTGTTCTGCATGCCTTACCCTTCCAAAGGGCTGGGATTATAGGCGTG\t69899:;<;=<=>==<5<5==<<<<;<<<<;<;;;;;<<;9<=<;98766\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:145\tUQ:i:145\tNM:i:5\tMD:Z:12C3T2G0G10T18\tCC:Z:=\tCP:i:136223688\tZS:Z:R\tZN:i:8\n" +
                "SimSeq_24fc9f\t369\t2\t136223688\t0\t50M\t=\t101837970\t-34385719\tCACGCCTATAATCCCAGCCCTTTGGAAGGGTAAGGCATGCAGAACACCTG\t66789;<=<9;<<;;;;;<;<<<<;<<<<==5<5<==>=<=;<;:99896\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:175\tUQ:i:175\tNM:i:6\tMD:Z:18A6G3C1G5G5T6\tZS:Z:R\tZN:i:8\n" +
                "SimSeq_24fc9f\t145\t2\t101837970\t53\t1S49M\t=\t170483133\t68645115\tATTGTTTTGTAACAGAGTCTCACTGTGTCACCCACGCTGAGGTACAGTGG\t6679:;;@;;#<;<;;<;;=<<;=;<;<>=<<<5<;=;;<;G;:::9975\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:121\tUQ:i:121\tNM:i:4\tMD:Z:2A5A0G28G10\tCC:Z:=\tCP:i:229443306\tZS:Z:R\tZN:i:5\n" +
                "SimSeq_24fc9f\t401\t2\t229443306\t0\t1S49M\t=\t170483133\t-58960221\tATTGTTTTGTAACAGAGTCTCACTGTGTCACCCACGCTGAGGTACAGTGG\t6679:;;@;;#<;<;;<;;=<<;=;<;<>=<<<5<;=;;<;G;:::9975\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:181\tUQ:i:181\tNM:i:6\tMD:Z:2T5A0G13C9G5A9\tZS:Z:R\tZN:i:5\n";


        OutputCollector<Text, Text> collector = mock(OutputCollector.class);
        Reporter reporter = mock(Reporter.class);

        NovoalignMatePairMapper mapper = new NovoalignMatePairMapper();
        mapper.setOutput(collector);
        mapper.setReporter(reporter);

        mapper.readAlignments(new BufferedReader(new StringReader(alignments)), null);

        verify(collector).collect(new Text("SimSeq_24fc9f-0"),
                new Text("SimSeq_24fc9f\t97\t2\t170483133\t21\t50M\t=\t101837970\t-68645115\tCAGGTGTTCTGCATGCCTTACCCTTCCAAAGGGCTGGGATTATAGGCGTG\t69899:;<;=<=>==<5<5==<<<<;<<<<;<;;;;;<<;9<=<;98766\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:145\tUQ:i:145\tNM:i:5\tMD:Z:12C3T2G0G10T18\tCC:Z:=\tCP:i:136223688\tZS:Z:R\tZN:i:8\tSEP\tSimSeq_24fc9f\t145\t2\t101837970\t53\t1S49M\t=\t170483133\t68645115\tATTGTTTTGTAACAGAGTCTCACTGTGTCACCCACGCTGAGGTACAGTGG\t6679:;;@;;#<;<;;<;;=<<;=;<;<>=<<<5<;=;;<;G;:::9975\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:121\tUQ:i:121\tNM:i:4\tMD:Z:2A5A0G28G10\tCC:Z:=\tCP:i:229443306\tZS:Z:R\tZN:i:5"));
        verify(collector).collect(new Text("SimSeq_24fc9f-1"),
                new Text("SimSeq_24fc9f\t97\t2\t170483133\t21\t50M\t=\t101837970\t-68645115\tCAGGTGTTCTGCATGCCTTACCCTTCCAAAGGGCTGGGATTATAGGCGTG\t69899:;<;=<=>==<5<5==<<<<;<<<<;<;;;;;<<;9<=<;98766\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:145\tUQ:i:145\tNM:i:5\tMD:Z:12C3T2G0G10T18\tCC:Z:=\tCP:i:136223688\tZS:Z:R\tZN:i:8\tSEP\tSimSeq_24fc9f\t401\t2\t229443306\t0\t1S49M\t=\t170483133\t-58960221\tATTGTTTTGTAACAGAGTCTCACTGTGTCACCCACGCTGAGGTACAGTGG\t6679:;;@;;#<;<;;<;;=<<;=;<;<>=<<<5<;=;;<;G;:::9975\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:181\tUQ:i:181\tNM:i:6\tMD:Z:2T5A0G13C9G5A9\tZS:Z:R\tZN:i:5"));
        verify(collector).collect(new Text("SimSeq_24fc9f-2"),
                new Text("SimSeq_24fc9f\t369\t2\t136223688\t0\t50M\t=\t101837970\t-34385719\tCACGCCTATAATCCCAGCCCTTTGGAAGGGTAAGGCATGCAGAACACCTG\t66789;<=<9;<<;;;;;<;<<<<;<<<<==5<5<==>=<=;<;:99896\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:175\tUQ:i:175\tNM:i:6\tMD:Z:18A6G3C1G5G5T6\tZS:Z:R\tZN:i:8\tSEP\tSimSeq_24fc9f\t145\t2\t101837970\t53\t1S49M\t=\t170483133\t68645115\tATTGTTTTGTAACAGAGTCTCACTGTGTCACCCACGCTGAGGTACAGTGG\t6679:;;@;;#<;<;;<;;=<<;=;<;<>=<<<5<;=;;<;G;:::9975\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:121\tUQ:i:121\tNM:i:4\tMD:Z:2A5A0G28G10\tCC:Z:=\tCP:i:229443306\tZS:Z:R\tZN:i:5"));
        verify(collector).collect(new Text("SimSeq_24fc9f-3"),
                new Text("SimSeq_24fc9f\t369\t2\t136223688\t0\t50M\t=\t101837970\t-34385719\tCACGCCTATAATCCCAGCCCTTTGGAAGGGTAAGGCATGCAGAACACCTG\t66789;<=<9;<<;;;;;<;<<<<;<<<<==5<5<==>=<=;<;:99896\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:175\tUQ:i:175\tNM:i:6\tMD:Z:18A6G3C1G5G5T6\tZS:Z:R\tZN:i:8\tSEP\tSimSeq_24fc9f\t401\t2\t229443306\t0\t1S49M\t=\t170483133\t-58960221\tATTGTTTTGTAACAGAGTCTCACTGTGTCACCCACGCTGAGGTACAGTGG\t6679:;;@;;#<;<;;<;;=<<;=;<;<>=<<<5<;=;;<;G;:::9975\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:181\tUQ:i:181\tNM:i:6\tMD:Z:2T5A0G13C9G5A9\tZS:Z:R\tZN:i:5"));
        verifyNoMoreInteractions(collector);

    }

    @Test
    public void testReadAlignments_properPairs() throws IOException {
        String alignments = "SimSeq_1a00a3\t163\t2\t36540548\t0\t5S45M\t=\t36540646\t147\tCTATGTCACTGATATGTCTAAGATCGACAGTGGGGTGTTAAAGTCTACCA\t,---+.-.,.,-...,.,...-/.--/-.-.-,,--,-----,-+--++-\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+- AS:i:118\tUQ:i:118\tNM:i:6\tMD:Z:3T4C7T2T6A14C3\tPQ:i:249\tSM:i:1\tAM:i:1\tZS:Z:R\tZN:i:4\n" +
                "SimSeq_1a00a3\t83\t2\t36540646\t0\t50M\t=\t36540548\t-147\tATGATTCTGGGGGGTCCTGTGTTGTGTGCATACATATTTAAGATGGTGAG\t,,+,,,+,+++,,,-,,-,-,..-.,.,,.-.,.-------,----,--+\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+-\tAS:i:131\tUQ:i:131\tNM:i:8\tMD:Z:4G6T8A3G7T7G3A2T2\tPQ:i:249\tSM:i:150\tAM:i:1\tZS:Z:R\tZN:i:4\n" +
                "SimSeq_1a00a3\t99\t2\t177663636\t2\t46M4S\t=\t177663746\t159\tCAGGTCTTCACCCAGCGGATCTAATAGAAATCTACAAGACTCTCCATGAC\t,,++.---,-,,,-,,,,--,.....,..-.,--,--+-+-+-#+,,+,+\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+-\tAS:i:174\tUQ:i:174\tNM:i:7\tMD:Z:3C3G4A6C8C7G0A8\tPQ:i:299\tSM:i:0\tAM:i:0\tZS:Z:R\tZN:i:3\n" +
                "SimSeq_1a00a3\t147\t2\t177663746\t2\t50M\t=\t177663636\t-159\tATACGCGGGAGTAAAGGTCTCATCAGCAACTGTAGCAGAACAGACATTAT\t,-,,+,+,,,,.---,,.,.-./-.--..-.-/.,,.,..,-,.,.----\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+-\tAS:i:125\tUQ:i:125\tNM:i:8\tMD:Z:4T0T2A12C7A4A0A8A5\tPQ:i:299\tSM:i:150\tAM:i:0\tZS:Z:R\tZN:i:3";

        OutputCollector<Text, Text> collector = mock(OutputCollector.class);
        Reporter reporter = mock(Reporter.class);

        NovoalignMatePairMapper mapper = new NovoalignMatePairMapper();
        mapper.setOutput(collector);
        mapper.setReporter(reporter);

        mapper.readAlignments(new BufferedReader(new StringReader(alignments)), null);

        verify(collector).collect(new Text("SimSeq_1a00a3-0"),
                new Text("SimSeq_1a00a3\t83\t2\t36540646\t0\t50M\t=\t36540548\t-147\tATGATTCTGGGGGGTCCTGTGTTGTGTGCATACATATTTAAGATGGTGAG\t,,+,,,+,+++,,,-,,-,-,..-.,.,,.-.,.-------,----,--+\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+-\tAS:i:131\tUQ:i:131\tNM:i:8\tMD:Z:4G6T8A3G7T7G3A2T2\tPQ:i:249\tSM:i:150\tAM:i:1\tZS:Z:R\tZN:i:4\tSEP\tSimSeq_1a00a3\t163\t2\t36540548\t0\t5S45M\t=\t36540646\t147\tCTATGTCACTGATATGTCTAAGATCGACAGTGGGGTGTTAAAGTCTACCA\t,---+.-.,.,-...,.,...-/.--/-.-.-,,--,-----,-+--++-\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+- AS:i:118\tUQ:i:118\tNM:i:6\tMD:Z:3T4C7T2T6A14C3\tPQ:i:249\tSM:i:1\tAM:i:1\tZS:Z:R\tZN:i:4"));
        verify(collector).collect(new Text("SimSeq_1a00a3-1"),
                new Text("SimSeq_1a00a3\t99\t2\t177663636\t2\t46M4S\t=\t177663746\t159\tCAGGTCTTCACCCAGCGGATCTAATAGAAATCTACAAGACTCTCCATGAC\t,,++.---,-,,,-,,,,--,.....,..-.,--,--+-+-+-#+,,+,+\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+-\tAS:i:174\tUQ:i:174\tNM:i:7\tMD:Z:3C3G4A6C8C7G0A8\tPQ:i:299\tSM:i:0\tAM:i:0\tZS:Z:R\tZN:i:3\tSEP\tSimSeq_1a00a3\t147\t2\t177663746\t2\t50M\t=\t177663636\t-159\tATACGCGGGAGTAAAGGTCTCATCAGCAACTGTAGCAGAACAGACATTAT\t,-,,+,+,,,,.---,,.,.-./-.--..-.-/.,,.,..,-,.,.----\tPG:Z:novoalign\tRG:Z:RG1\tPU:Z:ILLUMINA\tLB:Z:LC_1\tZO:Z:+-\tAS:i:125\tUQ:i:125\tNM:i:8\tMD:Z:4T0T2A12C7A4A0A8A5\tPQ:i:299\tSM:i:150\tAM:i:0\tZS:Z:R\tZN:i:3"));
        verifyNoMoreInteractions(collector);

    }

    @Test
    public void testReadAligmnents_oneReadUnmapped() throws Exception {
        String alignments = "SimSeq_24fb76\t69\t2\t214231607\t0\t*\t=\t214231607\t0\tGACAGAAAACCAAACATTGCGTCTTCTCTCTTATAGGTGGGAATTGTAAA\t797::;;;<;BF==D>E=<>==;<<;<;<;=;9;=<;<<;<;<;;98966\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tZS:Z:NM\n" +
                "SimSeq_24fb76\t137\t2\t214231607\t0\t50M\t=\t214231607\t0\tACCCATTAACTCATCATTTACAGTAGGTATATCTCAGAATGGTATCCCTC\t86?85::<<:<<=<;<<===<=<=<<6==B<<<=;<H<=;;:=,;98866\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:116\tUQ:i:116\tNM:i:4\tMD:Z:22T3A8C0T13\tCC:Z:=\tCP:i:18496541\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t409\t2\t18496541\t0\t49M1S\t=\t214231607\t0\tGAGGGATACCATTCTGAGATATACCTACTGTAAATGATGAGTTAATGGGT\t66889;,=:;;=<H<;=<<<B==6<<=<=<===<<;<=<<:<<::58?68\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:118\tUQ:i:118\tNM:i:3\tMD:Z:8G5G12A21\tCC:Z:=\tCP:i:217325427\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t409\t2\t217325427\t0\t50M\t=\t214231607\t0\tGAGGGATACCATTCTGAGATATACCTACTGTAAATGATGAGTTAATGGGT\t66889;,=:;;=<H<;=<<<B==6<<=<=<===<<;<=<<:<<::58?68\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:8G4A0G12A22\tCC:Z:=\tCP:i:225130248\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t409\t2\t225130248\t0\t50M\t=\t214231607\t0\tGAGGGATACCATTCTGAGATATACCTACTGTAAATGATGAGTTAATGGGT\t66889;,=:;;=<H<;=<<<B==6<<=<=<===<<;<=<<:<<::58?68\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:8G4A0G12A22\tCC:Z:=\tCP:i:114544422\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t393\t2\t114544422\t0\t50M\t=\t214231607\t0\tACCCATTAACTCATCATTTACAGTAGGTATATCTCAGAATGGTATCCCTC\t86?85::<<:<<=<;<<===<=<=<<6==B<<<=;<H<=;;:=,;98866\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:22T12C0T4C8\tCC:Z:=\tCP:i:233461635\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t409\t2\t233461635\t0\t50M\t=\t214231607\t0\tGAGGGATACCATTCTGAGATATACCTACTGTAAATGATGAGTTAATGGGT\t66889;,=:;;=<H<;=<<<B==6<<=<=<===<<;<=<<:<<::58?68\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:13A0G12A9C12\tCC:Z:=\tCP:i:116087108\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t393\t2\t116087108\t0\t50M\t=\t214231607\t0\tACCCATTAACTCATCATTTACAGTAGGTATATCTCAGAATGGTATCCCTC\t86?85::<<:<<=<;<<===<=<=<<6==B<<<=;<H<=;;:=,;98866\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:22T12C0T4C8\tCC:Z:=\tCP:i:131480865\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t393\t2\t131480865\t0\t50M\t=\t214231607\t0\tACCCATTAACTCATCATTTACAGTAGGTATATCTCAGAATGGTATCCCTC\t86?85::<<:<<=<;<<===<=<=<<6==B<<<=;<H<=;;:=,;98866\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:22T12C0T4C8\tCC:Z:=\tCP:i:163255977\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t393\t2\t163255977\t0\t50M\t=\t214231607\t0\tACCCATTAACTCATCATTTACAGTAGGTATATCTCAGAATGGTATCCCTC\t86?85::<<:<<=<;<<===<=<=<<6==B<<<=;<H<=;;:=,;98866\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:22T12C0T4C8\tCC:Z:=\tCP:i:34733348\tZS:Z:R\tZN:i:123\n" +
                "SimSeq_24fb76\t409\t2\t34733348\t0\t50M\t=\t214231607\t0\tGAGGGATACCATTCTGAGATATACCTACTGTAAATGATGAGTTAATGGGT\t66889;,=:;;=<H<;=<<<B==6<<=<=<===<<;<=<<:<<::58?68\tPG:Z:novoalign\tRG:Z:RGID\tPU:Z:ILLUMINA\tLB:Z:SIM1\tAS:i:120\tUQ:i:120\tNM:i:4\tMD:Z:8G4A0G12A22\tZS:Z:R\tZN:i:123\n";

        OutputCollector<Text, Text> collector = mock(OutputCollector.class);
        Reporter reporter = mock(Reporter.class);


        NovoalignMatePairMapper mapper = new NovoalignMatePairMapper();
        mapper.setOutput(collector);
        mapper.setReporter(reporter);

        mapper.readAlignments(new BufferedReader(new StringReader(alignments)), null);

        verifyZeroInteractions(collector);
    }
}
