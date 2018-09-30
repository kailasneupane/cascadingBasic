package cascading.example.function;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;


/**
 * Created by serrorist on 9/29/18.
 */
public class Main {
    public static void main(String[] args) {

        /**
         * update first_name, middle_name, last_name fields via full_name field.
         */

        String sourcePath = "src/main/resources/cascading/example/function/function_source.txt";
        String sinkPath = "target/cascading/function/function_sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(new Fields("id", "full_name"), ";"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, ";"), sinkPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("nameSplitPipe");
        Fields newFields = new Fields("first_name", "middle_name", "last_name");
        pipe = new Each(pipe, new Insert(newFields, "", "", ""), Fields.ALL);
        pipe = new Each(
                pipe
                , newFields.append(new Fields("full_name")) //full_name field appended in newFields
                , new NameSplitFunction()
                , Fields.REPLACE
        );
        pipe = new Discard(pipe, new Fields("full_name"));

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit: \n" + sinkPath);

    }
}
