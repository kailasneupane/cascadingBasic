package cascading.random_stuffs.word_count.utilities;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Created by serrorist on 9/30/18.
 */
public class GroupCountBuffer extends BaseOperation implements Buffer {

    public GroupCountBuffer() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        TupleEntry tuple = new TupleEntry(tupleEntryIterator.next());
        tuple.setString("count", counter(tupleEntryIterator).toString());
        bufferCall.getOutputCollector().add(tuple);
    }

    //count the size of grouped tuples
    private Long counter(Iterator itr) {
        Long i = 1l; //first iteration was made while creating tuple from TupleEntry
        while (itr.hasNext()) {
            i++;
            itr.next();
        }
        return i;
    }
}
