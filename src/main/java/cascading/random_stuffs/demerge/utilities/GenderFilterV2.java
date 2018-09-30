package cascading.random_stuffs.demerge.utilities;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * Created by serrorist on 9/30/18.
 */
public class GenderFilterV2 extends BaseOperation implements Filter {

    private String filterInValue;

    public GenderFilterV2(String filterInValue) {
        this.filterInValue = filterInValue;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry = filterCall.getArguments();
        return !tupleEntry.getString("gender").equals(filterInValue);
    }
}
