package cascading.example.buffer;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.buffer.FirstNBuffer;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import java.io.Serializable;
import java.util.Comparator;


/**
 * Created by serrorist on 9/29/18.
 */
public class Main implements Serializable {
    public static void main(String[] args) {

        /**
         * If multiple row contain same mbr_id, take a single row with highest paid amount.
         */

        String sourcePath = "src/main/resources/cascading/example/buffer/buffer_source.txt";
        String sinkPath = "target/cascading/buffer/buffer_sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ";"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, ";"), sinkPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("bufferExamplePipe");
        Fields groupingFields = new Fields("mbr_id");
        Fields sortFields = new Fields("paid_amount");
        sortFields.setComparator("paid_amount", new CustomComparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return Integer.valueOf((String) o1).compareTo(Integer.valueOf((String) o2));
            }
        });
        pipe = new GroupBy(pipe, groupingFields, sortFields, true);
        pipe = new Every(pipe, new FirstNBuffer(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit: \n" + sinkPath);

    }

}
