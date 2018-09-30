package cascading.example.buffer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Created by serrorist on 9/29/18.
 */
public class CustomBuffer extends BaseOperation implements Buffer {
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
        /**
         * we can fetch all grouped tuples via iterator and manipulate them
         * but for now we need only the first tupleEntry
         */
        bufferCall.getOutputCollector().add(new TupleEntry(iterator.next()));
    }
}
