package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int groupByFieldIndex;
    private final Type groupByFieldType;
    private final int aggregateFieldIndex;
    private final Op aggOperator;
    private Map<Field, Integer> tuplesByGroup;
    private TupleIterator tupleIterator;
    private boolean grouping = true;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT)
            throw new IllegalArgumentException(" Strings support only COUNT operations");
        if (gbfield == NO_GROUPING)
            grouping = false;
        this.tuplesByGroup = new HashMap<>();
        this.groupByFieldIndex = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.aggOperator = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField;
        if (grouping)
            groupByField = tup.getField(this.groupByFieldIndex);
        else
            groupByField = new StringField("", 1);
        StringField aggregateField = (StringField) tup.getField(this.aggregateFieldIndex);

        if (this.tuplesByGroup.containsKey(groupByField)) {
            int currentCount = this.tuplesByGroup.get(groupByField);
            this.tuplesByGroup.put(groupByField, currentCount + 1);
        } else {
            this.tuplesByGroup.put(groupByField, 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        if (grouping)
            computerAggregateWithGrouping();
        else
            computerAggregate();
        return tupleIterator;
    }

    private void computerAggregate() {
        // create a Tuple with schema [ AggResultValue ]
        TupleDesc aggregateTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        // a list of the result to create Iterator from
        List<Tuple> aggregatedResult = new ArrayList<>(1);
        StringField key = new StringField("", 1);
        // compute the COUNT aggregation
        Tuple groupAggregate = new Tuple(aggregateTupleDesc);
        groupAggregate.setField(0, new IntField(tuplesByGroup.get(key)));
        // add aggregate tuple to list
        aggregatedResult.add(groupAggregate);
        // assign the iterator
        this.tupleIterator = new TupleIterator(aggregateTupleDesc, aggregatedResult);
    }

    private void computerAggregateWithGrouping() {
        // create a Tuple with schema [ AggKeyValue, AggResultValue ]
        TupleDesc aggregateTupleDesc = TupleDesc.
                merge(new TupleDesc(new Type[]{this.groupByFieldType}), new TupleDesc(new Type[]{Type.INT_TYPE}));
        // a list of the result to create Iterator from
        List<Tuple> aggregatedResult = new ArrayList<>(this.tuplesByGroup.keySet().size());
        // compute the COUNT aggregation
        for (Field key : this.tuplesByGroup.keySet()) {
            Tuple groupAggregate = new Tuple(aggregateTupleDesc);
            groupAggregate.setField(0, key);
            groupAggregate.setField(1, new IntField(tuplesByGroup.get(key)));
            // add aggregate tuple to list
            aggregatedResult.add(groupAggregate);
        }
        // assign the iterator
        this.tupleIterator = new TupleIterator(aggregateTupleDesc, aggregatedResult);
    }
}
