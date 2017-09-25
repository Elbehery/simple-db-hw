package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int groupByFieldIndex;
    private final Type groupByFieldType;
    private final int aggregateFieldIndex;
    private final Op aggOperator;
    private Map<Field, List<IntField>> tuplesByGroup;
    private TupleIterator tupleIterator;
    private boolean grouping = true;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group-by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (gbfield == NO_GROUPING)
            grouping = false;
        this.tuplesByGroup = new HashMap<>();
        this.groupByFieldIndex = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.aggOperator = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField;
        if (grouping)
            groupByField = tup.getField(this.groupByFieldIndex);
        else
            groupByField = new IntField(1);
        IntField aggregateField = (IntField) tup.getField(this.aggregateFieldIndex);

        if (this.tuplesByGroup.containsKey(groupByField)) {
            this.tuplesByGroup.get(groupByField).add(aggregateField);
        } else {
            List<IntField> dummyList = new LinkedList<>();
            dummyList.add(aggregateField);
            this.tuplesByGroup.put(groupByField, dummyList);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        if (grouping)
            computerAggregateWithGrouping();
        else
            computeAggregate();
        return tupleIterator;
    }

    private void computeAggregate() {
        TupleDesc aggregateTupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        List<Tuple> resultList = new ArrayList<>(1);
        Tuple groupAggregate = new Tuple(aggregateTupleDesc);
        IntField key = new IntField(1);

        switch (this.aggOperator) {
            case MIN:
                int min = Integer.MAX_VALUE;
                for (IntField value : this.tuplesByGroup.get(key)) {
                    if (value.getValue() < min) {
                        min = value.getValue();
                    }
                }
                groupAggregate.setField(0, new IntField(min));
                break;
            case MAX:
                int max = Integer.MIN_VALUE;
                for (IntField value : this.tuplesByGroup.get(key)) {
                    if (value.getValue() > max) {
                        max = value.getValue();
                    }
                }
                groupAggregate.setField(0, new IntField(max));
                break;
            case SUM:
                int sum = 0;
                for (IntField value : this.tuplesByGroup.get(key)) {
                    sum += value.getValue();
                }
                groupAggregate.setField(0, new IntField(sum));
                break;
            case AVG:
                sum = 0;
                for (IntField value : this.tuplesByGroup.get(key)) {
                    sum += value.getValue();
                }
                int avg = sum / this.tuplesByGroup.get(key).size();
                groupAggregate.setField(0, new IntField(avg));
                break;
        }

        // add aggregate tuple to list
        resultList.add(groupAggregate);
        // assign the iterator
        this.tupleIterator = new TupleIterator(aggregateTupleDesc, resultList);
    }

    private void computerAggregateWithGrouping() {
        TupleDesc aggregateTupleDesc = TupleDesc.
                merge(new TupleDesc(new Type[]{this.groupByFieldType}), new TupleDesc(new Type[]{Type.INT_TYPE}));

        List<Tuple> aggregatedResult = new LinkedList<>();

        for (Field key : this.tuplesByGroup.keySet()) {
            Tuple groupAggregate = new Tuple(aggregateTupleDesc);
            groupAggregate.setField(0, key);

            switch (this.aggOperator) {
                case MIN:
                    int min = Integer.MAX_VALUE;
                    for (IntField value : this.tuplesByGroup.get(key)) {
                        if (value.getValue() < min) {
                            min = value.getValue();
                        }
                    }
                    groupAggregate.setField(1, new IntField(min));
                    break;
                case MAX:
                    int max = Integer.MIN_VALUE;
                    for (IntField value : this.tuplesByGroup.get(key)) {
                        if (value.getValue() > max) {
                            max = value.getValue();
                        }
                    }
                    groupAggregate.setField(1, new IntField(max));
                    break;
                case SUM:
                    int sum = 0;
                    for (IntField value : this.tuplesByGroup.get(key)) {
                        sum += value.getValue();
                    }
                    groupAggregate.setField(1, new IntField(sum));
                    break;
                case AVG:
                    sum = 0;
                    for (IntField value : this.tuplesByGroup.get(key)) {
                        sum += value.getValue();
                    }
                    int avg = sum / this.tuplesByGroup.get(key).size();
                    groupAggregate.setField(1, new IntField(avg));
                    break;
            }
            // add aggregate tuple to list
            aggregatedResult.add(groupAggregate);
        }
        // assign the iterator
        this.tupleIterator = new TupleIterator(aggregateTupleDesc, aggregatedResult);
    }
}