package edu.ohsu.sonmezsysbio.svpipeline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import edu.ohsu.sonmezsysbio.svpipeline.command.*;

import java.io.IOException;

public class SVPipeline
{
    public static void main( String[] args ) throws IOException {
        JCommander jc = buildJCommander();

        try {
            jc.parse(args);
            String parsedCommand = jc.getParsedCommand();

            SVPipelineCommand command = (SVPipelineCommand) jc.getCommands().get(parsedCommand).getObjects().get(0);
            command.run();

        } catch (ParameterException pe) {
            System.err.println(pe.getMessage());
            jc.usage();
        }
    }

    protected static JCommander buildJCommander() {
        JCommander jc = new JCommander(new CommanderMain());
        CommandNovoalignMatePair novoalignMatePair = new CommandNovoalignMatePair();
        jc.addCommand("novoalignMatePair", novoalignMatePair);
        CommandPileupDeletionScores pileupDeletionScores = new CommandPileupDeletionScores();
        jc.addCommand("pileupDeletionScores", pileupDeletionScores);

        CommandNovoalignSingleEnds singleEnds  = new CommandNovoalignSingleEnds();
        jc.addCommand("alignSingleEnds", singleEnds);
        CommandPileupSingleEndDeletionScores pileupSingleEndDeletionScores = new CommandPileupSingleEndDeletionScores();
        jc.addCommand("pileupSingleEndDeletionScores", pileupSingleEndDeletionScores);

        CommandAverageWigOverSlidingWindow averageWigOverSlidingWindow = new CommandAverageWigOverSlidingWindow();
        jc.addCommand("averageWigOverSlidingWindow", averageWigOverSlidingWindow);

        CommandDumpReadsWithScores dumpReadsWithScores = new CommandDumpReadsWithScores();
        jc.addCommand("dumpReadsWithScores", dumpReadsWithScores);

        jc.setProgramName("SVPipeline");
        return jc;
    }
}
