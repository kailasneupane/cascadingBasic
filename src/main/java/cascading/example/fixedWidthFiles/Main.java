package cascading.example.fixedWidthFiles;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    public static void main(String[] args) {

        /**
         * Filter out the rows which contains gender value other than “M” or “F”.
         */

        String sourcePath = "src/main/resources/cascading/example/fixedWidthFiles/fixed_width.txt";
        String layoutPath = "src/main/resources/cascading/example/fixedWidthFiles/fixedWidthLayout.txt";
        String sinkPath = "target/cascading/fixedWidthFiles/fixedWidth_sink.txt";

        Tap sourceTap = new FileTap(new TextLine(new Fields("lines")), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, "|"), sinkPath, SinkMode.REPLACE);

        String regex = prepareRegexFromFixedWidthLayout(layoutPath);
        Fields fields = getFieldsFromFixedWidthLayout(layoutPath);


        Pipe pipe = new Pipe("fixedWidthPipe");
        pipe = new Each(pipe, new RegexParser(fields, regex));
        pipe = new Each(pipe, fields, new TrimValues(fields));

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed.\nPlease visit: \n" + sinkPath);

    }

    public static Fields getFieldsFromFixedWidthLayout(String layoutPath) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(layoutPath)));
            String line;
            Fields fields = null;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().startsWith("#")) {
                    if (fields == null)
                        fields = new Fields(line.split(";")[1]);
                    else
                        fields = fields.append(new Fields(line.split(";")[1]));
                }
            }
            System.out.println("Fields => " + fields.toString());
            return fields;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String prepareRegexFromFixedWidthLayout(String layoutPath) {

        //"kailashneupane".replaceAll("^(.{7})(.{7}).*", "$1;$2")
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(layoutPath)));
            String line;
            StringBuilder regex = new StringBuilder();
            regex.append("^");
            while ((line = reader.readLine()) != null) {
                if (!line.trim().startsWith("#")) {
                    regex.append("(.{");
                    regex.append(line.split(";")[2]);
                    regex.append("})");
                }
            }
            regex.append(".*");
            System.out.println("regex => " + regex);
            return String.valueOf(regex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }


    static class TrimValues extends BaseOperation implements Function {

        Fields fields = null;

        public TrimValues(Fields fields) {
            super(Fields.ARGS);
            this.fields = fields;
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
            TupleEntry inComming = functionCall.getArguments();
            TupleEntry outGoing = new TupleEntry(inComming);
            for (int i = 0; i < fields.size(); i++) {
                String value = outGoing.getString(fields.get(i));
                outGoing.setString(fields.get(i), value.trim());
            }
            functionCall.getOutputCollector().add(outGoing);
        }
    }
}
