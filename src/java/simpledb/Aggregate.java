package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final int aggregateFieldIndex;
    private final int groupingFieldIndex;
    private final Aggregator.Op operator;
    private TupleDesc tupleDesc;
    private OpIterator child;
    private TupleIterator tupleIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.aggregateFieldIndex = afield;
        this.groupingFieldIndex = gfield;
        this.operator = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.groupingFieldIndex == Aggregator.NO_GROUPING ? -1 : this.aggregateFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        if (this.groupingFieldIndex == Aggregator.NO_GROUPING)
            return null;

        return this.child.getTupleDesc().getFieldName(this.groupingFieldIndex);
    }

    public Type groupFieldType() {
        if (this.groupingFieldIndex == Aggregator.NO_GROUPING)
            return null;

        return this.child.getTupleDesc().getFieldType(this.groupingFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.aggregateFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.child.getTupleDesc().getFieldName(this.aggregateFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        this.child.open();
        this.tupleIterator = computeAndCacheAggregationResult();
        this.tupleIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (this.tupleIterator.hasNext())
            return this.tupleIterator.next();

        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.tupleIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (this.tupleDesc == null) {
            String aggColumnName = String.format("Agg ( %s ) ", this.child.getTupleDesc().getFieldName(aggregateFieldIndex));
            Type aggColumnType = this.child.getTupleDesc().getFieldType(aggregateFieldIndex);
            if (this.groupingFieldIndex == Aggregator.NO_GROUPING) {
                this.tupleDesc = new TupleDesc(new Type[]{aggColumnType}, new String[]{aggColumnName});
            } else {
                String groupColumnName = String.format("Group ( %s ) ", this.child.getTupleDesc().getFieldName(groupingFieldIndex));
                Type groupColumnType = this.child.getTupleDesc().getFieldType(groupingFieldIndex);
                this.tupleDesc = new TupleDesc(new Type[]{groupColumnType, aggColumnType}, new String[]{groupColumnName, aggColumnName});
            }
        }
        return this.tupleDesc;
    }

    public void close() {
        super.close();
        child.close();
        this.tupleIterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length > 0)
            this.child = children[0];
    }

    private TupleIterator computeAndCacheAggregationResult() throws DbException, TransactionAbortedException {
        String aggColumnName = String.format("Agg ( %s ) ", this.child.getTupleDesc().getFieldName(aggregateFieldIndex));
        Type aggColumnType = this.child.getTupleDesc().getFieldType(aggregateFieldIndex);
        Aggregator aggregator;
        switch (aggColumnType) {
            case INT_TYPE:
                aggregator = new IntegerAggregator(groupingFieldIndex, groupFieldType(), aggregateFieldIndex, operator);
                break;
            case STRING_TYPE:
                aggregator = new StringAggregator(groupingFieldIndex, groupFieldType(), aggregateFieldIndex, operator);
                break;
            default:
                throw new DbException(String.format("Illegal Column Type. AggColumn is : %s, and its type is : %s", aggColumnName, aggColumnType));
        }

        // feed the data into the aggregator to compute the aggregation
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        return (TupleIterator) aggregator.iterator();
    }
}
