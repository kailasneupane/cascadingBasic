package cascading.random_stuffs.age_from_dob;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
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
         * Calculate and store age on the basis of provided DOB in source file.
         */

        String sourcePath = "src/main/resources/cascading/random_stuffs/age_dob/dob_src.txt";
        String sinkPath = "target/random_stuffs/age_dob/age_sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ",", "\""), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, "|"), sinkPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("dobPipe");
        pipe = new Each(pipe, new Insert(new Fields("age"),""), Fields.ALL);
        pipe = new Each(pipe, new Fields("dob","age"), new AgeCalculatorFunction(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit: \n" + sinkPath);
    }
}
