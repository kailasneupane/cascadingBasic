package cascading.random_stuffs.demerge;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.random_stuffs.demerge.utilities.GenderFilterV2;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created by serrorist on 9/30/18.
 */
public class DeMergeMain {
    public static void main(String[] args) {

        /**
         *Demerge a single pipe to mPipe and fPipe on the basis of gender “M” and “F”.
         */

        String sourcePath = "src/main/resources/cascading/example/filter/filter_source.txt";
        String sinkMPath = "target/random_stuffs/demerge/male.txt";
        String sinkFPath = "target/random_stuffs/demerge/female.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ";"), sourcePath);
        Tap sinkMTap = new FileTap(new TextDelimited(true, "|"), sinkMPath, SinkMode.REPLACE);
        Tap sinkFTap = new FileTap(new TextDelimited(true, "|"), sinkFPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("genderFilterPipe");

        Pipe mPipe = new Pipe("malePipe", pipe);
        Pipe fPipe = new Pipe("femalePipe", pipe);

        mPipe = new Each(mPipe, new Fields("gender"), new GenderFilterV2("M"));
        fPipe = new Each(fPipe, new Fields("gender"), new GenderFilterV2("F"));

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(mPipe, sinkMTap)
                .addTailSink(fPipe, sinkFTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit:\n" + sinkMPath + "\n" + sinkFPath);
    }
}
