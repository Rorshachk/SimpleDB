package simpledb;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int buckets, min, max, tot, len;
    private int[] nums, left;
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.nums = new int[buckets + 1];
        this.left = new int[buckets + 1];
        this.tot = 0;

        len = (max - min) / buckets + 1;
        int cnt = 0;
        for(int i = min; i <= max; i += len){
            this.left[cnt] = i;
            cnt++;
        }

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int pos =  (v - min) / len;
        tot++;
        nums[pos]++;    
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        if(v < min){
            if(op.equals(Op.GREATER_THAN) || op.equals(Op.GREATER_THAN_OR_EQ) || op.equals(Op.NOT_EQUALS)) return 1.0;
            return 0.0;
        }
        if(v > max){
            if(op.equals(Op.LESS_THAN) || op.equals(Op.LESS_THAN_OR_EQ) || op.equals(Op.NOT_EQUALS)) return 1.0;
            return 0.0;
        }
        int pos = (v - min) / len;
        double b_f, b_ans;
        switch (op) {
            case EQUALS:
                if(pos == buckets - 1) return 1.0 * nums[pos] / (max - left[pos] + 1) / tot;
                else return 1.0 * nums[pos] / len / tot; 
            case NOT_EQUALS:
                if(pos == buckets - 1) return 1 - 1.0 * nums[pos] / (max - left[pos] + 1) / tot;
                else return 1 - 1.0 * nums[pos] / len / tot;
            case GREATER_THAN:
                b_f = nums[pos] * 1.0 / tot;
                if(pos == buckets - 1)  b_ans = b_f * (max - v) / (max - left[pos] + 1);
                else b_ans = b_f * (left[pos + 1] - v - 1) / len;
                for(int i = pos + 1; i < buckets; i++) b_ans += (nums[i] * 1.0 / tot);
            //    System.out.printf("%d %d\n", nums[buckets-1], left[buckets - 1]);
            //    System.out.printf("%.3f", b_ans);
                return b_ans;
            case GREATER_THAN_OR_EQ:
                b_f = nums[pos] * 1.0 / tot;
                if(pos == buckets - 1) b_ans = b_f * (max - v + 1) / (max - left[pos] + 1);
                else b_ans =  b_f * (left[pos + 1] - v) / len; 
                for(int i = pos + 1; i < buckets; i++) b_ans += (nums[i] * 1.0 / tot);
                return b_ans;
            case LESS_THAN:
                b_f = nums[pos] * 1.0 / tot;
                if(pos == buckets - 1) b_ans = b_f * (v - left[pos] + 1) / (max - left[pos] + 1);
                else b_ans = b_f * (v - left[pos]) / len;
                for(int i = 0; i < pos; i++) b_ans += (nums[i] * 1.0 / tot);
                return b_ans;
            case LESS_THAN_OR_EQ:
                b_f = nums[pos] * 1.0 / tot;
                if(pos == buckets - 1) b_ans = b_f * (v - left[pos] + 1) / (max - left[pos] + 1);
                else b_ans = b_f * (v - left[pos] + 1) / len;
                for(int i = 0; i < pos; i++) b_ans += (nums[i] * 1.0 / tot);
                return b_ans;
            default:
                return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
