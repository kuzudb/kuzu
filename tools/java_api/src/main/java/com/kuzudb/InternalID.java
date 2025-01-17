package com.kuzudb;

import java.util.Objects;

/**
 * InternalID type which stores the table_id and offset of a node/rel.
 */
public class InternalID {
    public long tableId;
    public long offset;

    /**
     * Create an InternalID from the given table_id and offset.
     *
     * @param tableId The table_id of the node/rel.
     * @param offset  The offset of the node/rel.
     */
    public InternalID(long tableId, long offset) {
        this.tableId = tableId;
        this.offset = offset;
    }

    /**
     * Compares this InternalID to another object for equality.
     * Two InternalID objects are considered equal if their tableId and offset are the same.
     *
     * @param obj The object to be compared for equality with this InternalID.
     * @return true if the specified object is equal to this InternalID, false otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true; // If comparing the same instance
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false; // Null or different class
        }
        InternalID other = (InternalID) obj;
        return this.tableId == other.tableId && this.offset == other.offset;
    }

    /**
     * Returns the hash code value for this InternalID.
     * The hash code is generated using the tableId and offset fields to ensure that
     * equal InternalID objects produce the same hash code.
     *
     * @return The hash code value for this InternalID.
     */
    @Override
    public int hashCode() {
        return Objects.hash(tableId, offset);
    }

    /**
     * Returns a string representation of this InternalID.
     *
     * @return A string representation of this InternalID.
     */
    @Override
    public String toString() {
        return "InternalID{" +
                "tableId=" + tableId +
                ", offset=" + offset +
                '}';
    }
}
