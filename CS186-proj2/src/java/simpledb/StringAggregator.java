package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<Field, Integer> countMap;
    private TupleDesc td;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.afield = afield;
        countMap = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field;
        int value = 0;
        if(gbfield == -1) field = new IntField(-1);
        else field = tup.getField(gbfield);
        if(this.what == Op.COUNT){
            if(countMap.get(field) == null) countMap.put(field, 1);
            else{
                value = countMap.get(field) + 1;
                countMap.put(field, value);
            }
        }else{
            throw new UnsupportedOperationException("It doesn't support this operator!");
        }
        if(td == null) td = tup.getTupleDesc();
        return ;
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    //    throw new UnsupportedOperationException("please implement me for proj2");
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc restd;
        if(gbfield == -1) restd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{td.getFieldName(afield)});
        else restd = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{td.getFieldName(gbfield), td.getFieldName(afield)});

        if(what == Op.COUNT){
            for(Map.Entry<Field, Integer> entry: countMap.entrySet()){
                Tuple tmptuple = new Tuple(restd);
            if(gbfield == -1)    tmptuple.setField(0, new IntField(entry.getValue()));
            else{
                tmptuple.setField(0, entry.getKey());
                tmptuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(tmptuple);
            }
            return new TupleIterator(restd, tuples);
        }else{
            throw new UnsupportedOperationException("It doesn't supprot this operator!");
        }
    }

}
