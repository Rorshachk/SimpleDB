package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    private int ioCostPerPage, ntups;
    private HeapFile file;
    private TupleDesc td;
    private HashMap<String, Integer[]> minmax;
    private HashMap<String, Object> name2his;
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        this.file = (HeapFile)Database.getCatalog().getDbFile(tableid);
        this.td = this.file.getTupleDesc();
        name2his = new HashMap<>();
        minmax = new HashMap<>();
        Transaction t = new Transaction();
        DbFileIterator itr = file.iterator(t.getId());
        create_min_max(itr);
        create_his(itr);
    }

    private void create_min_max(DbFileIterator itr){
        try{
            itr.open();
            while(itr.hasNext()){
                ntups++;
                Tuple t = itr.next();
                for(int i = 0; i < td.numFields(); i++){
                    Type type = td.getFieldType(i);
                    if(type.equals(Type.INT_TYPE)){
                        String name = td.getFieldName(i);
                        int value = ((IntField)t.getField(i)).getValue();
                        if(minmax.get(name) == null){
                            minmax.put(name, new Integer[]{value, value});
                        }else{
                            int maxValue = Math.max(value, minmax.get(name)[1]);
                            int minValue = Math.min(value, minmax.get(name)[0]);
                            minmax.put(name, new Integer[]{minValue, maxValue});
                        }
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void create_his(DbFileIterator itr){
        for(Map.Entry<String, Integer[]> entry : minmax.entrySet())
          name2his.put(entry.getKey(), new IntHistogram(NUM_HIST_BINS, entry.getValue()[0], entry.getValue()[1]));
        try{
            itr.rewind();
            while(itr.hasNext()){
                Tuple t = itr.next();
                for(int i = 0; i < td.numFields(); i++){
                    String name = td.getFieldName(i);
                    Type type = td.getFieldType(i);
                    if(type.equals(Type.INT_TYPE)){
                        int value = ((IntField)t.getField(i)).getValue();
                        IntHistogram his = (IntHistogram)name2his.get(name);
                        his.addValue(value);
                        name2his.put(name, his);
                    }else{
                        String value = ((StringField)t.getField(i)).getValue();
                        StringHistogram his;
                        if(name2his.get(name) == null) his = new StringHistogram(NUM_HIST_BINS);
                        else his = (StringHistogram)name2his.get(name);
                        his.addValue(value);
                        name2his.put(name, value);
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return file.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) Math.ceil(selectivityFactor * totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        String name = td.getFieldName(field);
        Type type = td.getFieldType(field);
        if(type.equals(Type.INT_TYPE))
          return ((IntHistogram)name2his.get(name)).avgSelectivity();
        else return ((StringHistogram)name2his.get(name)).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        String name = td.getFieldName(field);
        if(constant.getType().equals(Type.INT_TYPE)){
            int v = ((IntField)constant).getValue();
            IntHistogram his = (IntHistogram)name2his.get(name);
            return his.estimateSelectivity(op, v);
        }else{
            String s = ((StringField)constant).getValue();
            StringHistogram his = (StringHistogram)name2his.get(name);
            return his.estimateSelectivity(op, s);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.ntups;
    }

}
