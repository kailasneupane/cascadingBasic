package cascading.random_stuffs.word_count.utilities;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Created by serrorist on 9/30/18.
 */
public class RowSplitFunction extends BaseOperation implements Function {

    public RowSplitFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        TupleEntry tuple = new TupleEntry(tupleEntry);
        String[] row = tuple.getString("line").split("\\s+");
        for (String word : row) {
            word = word.replaceAll("[^a-zA-Z0-9]", ""); //removing special characters
            tuple.setString("line", word);
            functionCall.getOutputCollector().add(tuple);
        }
    }
}
