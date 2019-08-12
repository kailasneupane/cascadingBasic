package cascading.example.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Created by serrorist on 9/29/18.
 */
public class NameSplitFunction extends BaseOperation implements Function {

    public NameSplitFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        TupleEntry tuple = new TupleEntry(tupleEntry);
        String[] name = tupleEntry.getString("full_name").split("\\s+");
//logic applied
        tuple.setString("first_name", name[0]);

        String middleName = "";
        for(int i = 1;i<name.length -1 ; i++){
            middleName = middleName + name[i] + " ";
        }
        tuple.setString("middle_name", middleName.trim());
        tuple.setString("last_name", name[name.length - 1]);

        functionCall.getOutputCollector().add(tuple);  //sends resulting tuple to the pipe stream
    }
}
