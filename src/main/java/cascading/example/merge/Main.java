package cascading.example.merge;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created by serrorist on 9/30/18.
 */
public class Main {
    public static void main(String[] args) {

        /**
         * source1 and source2 have identical fields. so, we can apply merge
         */

        String source1Path = "src/main/resources/cascading/example/merge/merge_source1.txt";
        String source2Path = "src/main/resources/cascading/example/merge/merge_source2.txt";
        String sinkPath = "target/cascading/merge/merge_sink.txt";

        Tap source1Tap = new FileTap(new TextDelimited(true, ";"), source1Path);
        Tap source2Tap = new FileTap(new TextDelimited(true, "|"), source2Path);
        Tap sinkTap = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);

        Pipe pipe1 = new Pipe("pipe1");
        Pipe pipe2 = new Pipe("pipe2");

        Pipe finalPipe = new Merge(pipe1, pipe2);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe1, source1Tap)
                .addSource(pipe2, source2Tap)
                .addTailSink(finalPipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit: \n" + sinkPath);

    }
}
