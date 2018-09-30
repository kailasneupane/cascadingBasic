package cascading.example.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * Created by serrorist on 9/29/18.
 */
public class GenderFilter extends BaseOperation implements Filter {
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry = filterCall.getArguments();
        String gender = tupleEntry.getString("gender");
        return !(gender.equals("M") || gender.equals("F"));
    }
}
