package powertools.cdc;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.schema.CQLTypeParser.parse;

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

    private DroppedColumn resolveDropped(Row droppedColumn)
    {
        String keyspaceName = droppedColumn.getString("keyspace_name");
        String tableName = droppedColumn.getString("table_name");
        String columnName = droppedColumn.getString("column_name");
        String typeString = droppedColumn.getString("type");
        String kindString = droppedColumn.getString("kind");

        ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(kindString.toUpperCase());


        long dropped = droppedColumn.getTimestamp("dropped_time").getTime();
        AbstractType<?> type = CQLTypeParser.parse(keyspaceName, typeString, Types.none());

        // Here's hoping that this never gets used in DroppedColumn
        ColumnIdentifier identifierName = new ColumnIdentifier(ByteBufferUtil.bytes(0),type);
        int position = 0;

        ColumnMetadata columnMeta = new ColumnMetadata(keyspaceName, tableName, identifierName, type, position, kind);
        return new DroppedColumn(columnMeta, dropped);
    }

    private ColumnMetadata resolveColumn(Row column)
    {
        String keyspaceName = column.getString("keyspace_name");
        column.getColumnDefinitions().getType(1);
        ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(column.getString("kind").toUpperCase());
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

        // Why is this an issue with the dse.jar imported?
        if (typeName.contains("insights") || typeName.contains("nodesync") || typeName.contains("chunk") || typeName.contains("ace")){
            return null;
        }
        typeBuilder.add(name, fieldNames, fieldTypes);
        Types types = typeBuilder.build();;

        AbstractType<?> type = parse(keyspaceName, typeName, types);

        return new ColumnMetadata(keyspaceName,
                                    tableName,
                                    //ColumnIdentifier.getInterned(columnNameBytes, columnName),
                                    ColumnIdentifier.getInterned(type, columnNameBytes, columnName),
                                    CQLTypeParser.parse(keyspaceName, typeName, Types.none()),
                                    position,
                                    kind);
    }

    private TableMetadata resolveTable(Row table)
    {
        String keyspaceName = table.getString("keyspace_name");
        String tableName = table.getString("table_name");

        UUID tableUUID = table.getUUID("id");

        //optional?
        TableId tableId = TableId.fromUUID(tableUUID);

        TableMetadata.Builder tableBuilder = TableMetadata.builder(keyspaceName, tableName, tableId);

        Set<String> flagStrings= table.getSet("flags", String.class);
        EnumSet<TableMetadata.Flag> flags = EnumSet.copyOf(flagStrings.stream()
                .map(String::toUpperCase)
                .map(TableMetadata.Flag::valueOf)
                .collect(Collectors.toSet()));
        tableBuilder.flags(flags);


        List<ColumnMetadata> columns = new ArrayList<>();

        List<Row> rows = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", "system_schema", SchemaKeyspace.COLUMNS),
                keyspaceName,
                tableName)
                .all();
        for (Row row : rows)
        {
            ColumnMetadata columnDef = resolveColumn(row);
            if (columnDef == null){
                continue;
            }
            columns.add(columnDef);
        }
        tableBuilder.addColumns(columns);

        Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap<>();
        List<Row> droppedRows = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", "system_schema", SchemaKeyspace.DROPPED_COLUMNS),
                keyspaceName,
                tableName)
                .all();
        for (Row row : droppedRows)
        {
            // I am unsure about using unsafe here
            droppedColumns.put(row.getBytesUnsafe("column_name"), resolveDropped(row));
        }

        tableBuilder.droppedColumns(droppedColumns);

        tableBuilder.params(TableParams.builder().cdc(table.getBool("cdc")).build());

        return tableBuilder.build();
        /*
        return CFMetaData.create(keyspaceName,
         tableName,
         tableUUID,
         flags.contains(CFMetaData.Flag.DENSE),
         flags.contains(CFMetaData.Flag.COMPOUND),
         flags.contains(CFMetaData.Flag.SUPER),
         flags.contains(CFMetaData.Flag.COUNTER),
         false,
         columns,
         new RandomPartitioner())
        .droppedColumns(droppedColumns)
         .params(TableParams.builder().cdc(table.getBool("cdc")).build());
         */
    }

    private KeyspaceMetadata resolveKeyspace(Row keyspace)
    {
        String keyspaceName = keyspace.getString("keyspace_name");
        List<Row> tables = session.execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", "system_schema", SchemaKeyspace.TABLES),
         keyspaceName).all();
        Tables.Builder builder = Tables.builder();
        for (Row row : tables)
        {
            builder.add(resolveTable(row));
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
