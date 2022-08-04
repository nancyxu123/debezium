package io.debezium.connector.spanner;

import io.debezium.annotation.Immutable;
import io.debezium.schema.DataCollectionId;

/*
 * A simple identifier for collections in Spanner
 */
@Immutable
final class CollectionId implements DataCollectionId {
    private final String projectId;
    private final String instanceId;
    private final String databaseId;
    private final String tableName;

    public CollectionId(String projectId, String instanceId, String databaseId, String tableName) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
        this.tableName = tableName;
        assert this.projectId != null;
        assert this.instanceId != null;
        assert this.databaseId != null;
        assert this.tableName != null;
    }

    public String projectId() {
        return projectId;
    }

    public String instanceId() {
        return instanceId;
    }

    public String databaseId() {
        return databaseId;
    }

    public String tableName() {
        return tableName;
    }

    @Override
    public String identifier() {
        return projectId + "." + instanceId + "." + databaseId + "." + tableName;
    }

    @Override
    public int hashCode() {
        return tableName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof CollectionId) {
            CollectionId that = (CollectionId) obj;
            return this.projectId.equals(that.projectId)
                    && this.instanceId.equals(that.instanceId)
                    && this.databaseId.equals(that.databaseId)
                    && this.tableName.equals(that.tableName);
        }
        return false;
    }

    @Override
    public String toString() {
        return identifier();
    }
}
