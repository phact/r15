package org.apache.cassandra.cdc;

import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.schema.*;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.schema.CQLTypeParser.parse;
import static org.apache.cassandra.schema.SchemaKeyspace.TYPES;

/**
 * This class allows us to read a schema from a running Cassandra instance, and build a local Schema instance from it.
 */
public class RemoteSchema
{
    final Cluster cluster;
    final Session session;

    public RemoteSchema(Cluster cluster)
    {
        this.cluster = cluster;
        session = cluster.connect("system_schema");
    }

    private CFMetaData.DroppedColumn resolveDropped(Row droppedColumn)
    {
        String keyspaceName = droppedColumn.getString("keyspace_name");
        String columnName = droppedColumn.getString("column_name");
        String type = droppedColumn.getString("type");
        long dropped = droppedColumn.getTimestamp("dropped_time").getTime();
        return new CFMetaData.DroppedColumn(columnName,
                                            CQLTypeParser.parse(keyspaceName, type, Types.none()),
                                            dropped);
    }

    private ColumnDefinition resolveColumn(Row column)
    {
        String keyspaceName = column.getString("keyspace_name");
        column.getColumnDefinitions().getType(1);
        ColumnDefinition.Kind kind = ColumnDefinition.Kind.valueOf(column.getString("kind").toUpperCase());
        int position = column.getInt("position");
        String columnName = column.getString("column_name");
        ByteBuffer columnNameBytes = column.getBytes("column_name_bytes");
        String tableName = column.getString("table_name");
        String typeName = column.getString("type");

        Types.RawBuilder typeBuilder = org.apache.cassandra.schema.Types.rawBuilder(keyspaceName);
        List<ColumnDefinitions.Definition> columnDefs = column.getColumnDefinitions().asList();
        //Doubtful if this is a thing but let's try
        //String name = column.getString("type_name");
        String name = column.getString("type");
        List<String> fieldNames = new ArrayList<>();
        List<String> fieldTypes = new ArrayList<>();
        fieldNames.add(columnName);
        fieldTypes.add(typeName);
        if (typeName.contains("insights") || typeName.contains("nodesync") || typeName.contains("chunk") || typeName.contains("ace")){
            return null;
        }
        typeBuilder.add(name, fieldNames, fieldTypes);
        Types types = typeBuilder.build();;

        AbstractType<?> type = parse(keyspaceName, typeName, types);
        return new ColumnDefinition(keyspaceName,
                                    tableName,
                                    //ColumnIdentifier.getInterned(columnNameBytes, columnName),
                                    ColumnIdentifier.getInterned(type, columnNameBytes, columnName),
                                    CQLTypeParser.parse(keyspaceName, typeName, Types.none()),
                                    position,
                                    kind);
    }

    private CFMetaData resolveCF(Row table)
    {
        String keyspaceName = table.getString("keyspace_name");
        String tableName = table.getString("table_name");
        UUID cfId = table.getUUID("id");
        EnumSet<CFMetaData.Flag> flags = EnumSet.copyOf(CFMetaData.flagsFromStrings(table.getSet("flags", String.class)));
        List<ColumnDefinition> columns = new ArrayList<>();
        Map<ByteBuffer, CFMetaData.DroppedColumn> droppedColumns = new HashMap<>();

        {
            List<Row> rows = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", "system_schema", SchemaKeyspace.COLUMNS),
             keyspaceName,
             tableName)
                                 .all();
            for (Row row : rows)
            {
                ColumnDefinition columnDef = resolveColumn(row);
                if (columnDef == null){
                    continue;
                }
                columns.add(columnDef);
            }
        }

        {
            List<Row> rows = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", "system_schema", SchemaKeyspace.DROPPED_COLUMNS),
             keyspaceName,
             tableName)
                              .all();
            for (Row row : rows)
            {
                // I am unsure about this
                droppedColumns.put(row.getBytesUnsafe("column_name"), resolveDropped(row));
            }
        }

        return CFMetaData.create(keyspaceName,
         tableName,
         cfId,
         flags.contains(CFMetaData.Flag.DENSE),
         flags.contains(CFMetaData.Flag.COMPOUND),
         flags.contains(CFMetaData.Flag.SUPER),
         flags.contains(CFMetaData.Flag.COUNTER),
         false,
         columns,
         new RandomPartitioner())
        .droppedColumns(droppedColumns)
         .params(TableParams.builder().cdc(table.getBool("cdc")).build());
    }

    private KeyspaceMetadata resolveKeyspace(Row keyspace)
    {
        String keyspaceName = keyspace.getString("keyspace_name");
        List<Row> tables = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", "system_schema", SchemaKeyspace.TABLES),
         keyspaceName).all();
        Tables.Builder builder = Tables.builder();
        for (Row row : tables)
        {
            builder.add(resolveCF(row));
        }
        Map<String, String> replication = keyspace.getMap("replication", String.class, String.class);
        if (replication.get("class").contains("Everywhere")){
            return null;
        }
        return KeyspaceMetadata.create(keyspaceName,
         KeyspaceParams.create(keyspace.getBool("durable_writes"),
          keyspace.getMap("replication", String.class, String.class)),
         builder.build());
    }

    public void load()
    {
        List<Row> keyspaces = session.execute(String.format("SELECT * FROM %s.%s", "system_schema", SchemaKeyspace.KEYSPACES))
                               .all();
        for (Row row : keyspaces)
        {
            KeyspaceMetadata keyspace = resolveKeyspace(row);
            if (keyspace == null){
                continue;
            }
            Schema.instance.load(keyspace);
        }
    }
}
