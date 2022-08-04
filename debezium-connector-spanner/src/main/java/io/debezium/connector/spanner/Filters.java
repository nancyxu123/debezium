/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import java.util.function.Predicate;

/**
 * A utility that is contains various filters for acceptable database names, {@link CollectionId}s,
 * and fields.
 *
 * @author Randall Hauch
 */
public final class Filters {

    /**
     * Create an instance of the filters.
     *
     * @param config the configuration; may not be null
     */
    private final Predicate<CollectionId> collectionFilter;

    public Filters() {
        collectionFilter = (id) -> true;
    }

    /**
     * Get the predicate function that determines whether the given collection is to be included.
     *
     * @return the collection filter; never null
     */
    public Predicate<CollectionId> collectionFilter() {
        return collectionFilter;
    }
}
