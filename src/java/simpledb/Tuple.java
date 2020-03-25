package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private final Field[] fieldsValues;
    // TODO : refactor
    private TupleDesc tupleDesc;
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td == null)
            throw new NullPointerException("Tuple Schema can not be NULL ");
        this.fieldsValues = new Field[td.numFields()];
        this.tupleDesc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i >= this.fieldsValues.length)
            throw new IndexOutOfBoundsException("Index is OutOfRange");

        this.fieldsValues[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if (i < 0 || i >= this.fieldsValues.length)
            throw new IndexOutOfBoundsException("Index is OutOfRange");

        return this.fieldsValues[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Field field : fieldsValues) {
            builder.append(field.toString()).append("\t");
        }

        return builder.toString().trim();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {

        return new Iterator<Field>() {
            int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < fieldsValues.length;
            }

            @Override
            public Field next() {

                Field result;
                result = fieldsValues[currentIndex];
                currentIndex++;
                return result;
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = td;
    }
}
