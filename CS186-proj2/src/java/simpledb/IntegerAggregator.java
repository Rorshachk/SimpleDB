package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<Field, Integer> minMap, maxMap, sumMap, countMap;
    private TupleDesc td;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.afield = afield;
        switch(what){
            case MAX:
                maxMap = new HashMap<Field, Integer>();
                break;
            case MIN:
                minMap = new HashMap<Field, Integer>();
                break;
            case COUNT:
                countMap = new HashMap<Field, Integer>();
                break;
            case AVG:
                sumMap = new HashMap<Field, Integer>();
                countMap = new HashMap<Field, Integer>();
                break;
            case SUM:
                sumMap = new HashMap<Field, Integer>();
                break;
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field;
        int value;
        if(gbfield == -1) field = new IntField(-1);
        else field = tup.getField(gbfield);
        value = ((IntField)tup.getField(afield)).getValue();
        switch(what){
            case MAX:
                if(maxMap.get(field) == null) maxMap.put(field, value);
                else{
                    value = Math.max(value, maxMap.get(field));
                    maxMap.put(field, value);
                }
                break;
            case AVG:
                if(sumMap.get(field) == null){
                    sumMap.put(field, value);
                    countMap.put(field, 1);
                }else{
                    value += sumMap.get(field);
                    sumMap.put(field, value);
                    value = countMap.get(field) + 1;
                    countMap.put(field, value);
                }
                break;
            case SUM:
                if(sumMap.get(field) == null)  sumMap.put(field, value);
                else{
                    value += sumMap.get(field);
                    sumMap.put(field, value);
                }
                break;
            case MIN:
                if(minMap.get(field) == null) minMap.put(field, value);
                else{
                    value = Math.min(value, minMap.get(field));
                    minMap.put(field, value);
                }
                break;
            case COUNT:
                if(countMap.get(field) == null) countMap.put(field, 1);
                else{
                    value = countMap.get(field) + 1;
                    countMap.put(field, value);
                }
                break;
        }
        if(td == null) td = tup.getTupleDesc();
        return ;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    
    public DbIterator iterator() {
        // some code goes here
    //    throw new
    //    UnsupportedOperationException("please implement me for proj2");
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        HashMap<Field, Integer> map = new HashMap<Field, Integer>();
        TupleDesc restd;
        if(gbfield == -1) restd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{td.getFieldName(afield)});
        else restd = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{td.getFieldName(gbfield), td.getFieldName(afield)});
        switch(what){
            case MAX:
                map = maxMap;
                break;
            case MIN:
                map = minMap;
                break;
            case COUNT:
                map = countMap;
                break;
            case AVG:
                for(Map.Entry<Field, Integer> entry : countMap.entrySet())
                    map.put(entry.getKey(), sumMap.get(entry.getKey()) / entry.getValue());
                break;
            case SUM:
                map = sumMap;
                break;
        }
        for(Map.Entry<Field, Integer> entry : map.entrySet()){
            Tuple tmptuple = new Tuple(restd);
            if(gbfield == -1)    tmptuple.setField(0, new IntField(entry.getValue()));
            else{
                tmptuple.setField(0, entry.getKey());
                tmptuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(tmptuple);
        }
        return new TupleIterator(restd, tuples);
    }
}
