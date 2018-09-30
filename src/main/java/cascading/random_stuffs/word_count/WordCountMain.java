package cascading.random_stuffs.word_count;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.random_stuffs.word_count.utilities.GroupCountBuffer;
import cascading.random_stuffs.word_count.utilities.RowSplitFunction;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created by serrorist on 9/30/18.
 */
public class WordCountMain {
    public static void main(String[] args) {

        String sourcePath = "src/main/resources/cascading/random_stuffs/word_count/pink_floyd_lyrics_shine_on_you_crazy_diamond_i-v.txt";
        String sinkPath = "target/random_stuff/word_count/pink_floyd.txt";

        Tap sourceTap = new FileTap(new TextLine(new Fields("line")), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(new Fields("line", "count"), "|"), sinkPath, SinkMode.REPLACE);

        Pipe wcPipe = new Pipe("wcPipe");
        wcPipe = new Each(wcPipe, new Insert(new Fields("count"), "1"), Fields.ALL);
        wcPipe = new Each(wcPipe, new Fields("line"), new RowSplitFunction(), Fields.REPLACE);
        wcPipe = new GroupBy(wcPipe, new Fields("line"));
        //method 1: (using built in count)
        //wcPipe = new Every(wcPipe,new Fields("line"), new Count(new Fields("count")),new Fields("line","count"));

        //method 2:(using custom group count buffer)
        wcPipe = new Every(wcPipe, new Fields("count"), new GroupCountBuffer(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(wcPipe, sourceTap)
                .addTailSink(wcPipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit " + sinkPath);
    }
}
