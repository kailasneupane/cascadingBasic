package cascading.random_stuffs.longest_name;

import cascading.example.buffer.CustomBuffer;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
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
         * From multiple records having same SSN, take only one tuple with longest name.
         * Filter out the records if ssn is not 9 digits. Source Fields: ssn, name
         */

        String sourcePath = "src/main/resources/cascading/random_stuffs/long_name/src.txt";
        String sinkPath = "target/random_stuffs/long_name/sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ";"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, ";"), sinkPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("extract_longNameOnly");
        //filter out non 9 digit ssn using built in filter ExpressionFilter
        pipe = new Each(pipe, new Fields("ssn"), new ExpressionFilter("ssn.length()!=9", String.class));
        pipe = new GroupBy(pipe, new Fields("ssn"), new Fields("name"), true);
        //taking first tuple via buffer
        pipe = new Every(pipe, new CustomBuffer(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit: \n" + sinkPath);
    }
}
