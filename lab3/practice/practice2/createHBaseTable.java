/*** createHBaseTable.java ***/
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class createHBaseTable {
    public static void main(String[] args) throws Exception {
        // TableName tableName = TableName.valueOf("s111526005");
		// String ColumnFamily= "MB";
		// String newColumnFamilies[]= {ColumnFamily};
		TableName tableName = TableName.valueOf(args[0]);
        String newColumnFamilies[] = Arrays.copyOfRange(args, 1, args.length);
		ArrayList<ColumnFamilyDescriptor> newColumnFamilyDescriptors = new ArrayList<>();


        TableDescriptor tableDescriptor = null;
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        /* Delete the table if it exists */
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + " exists");
            System.out.println("disabling " + tableName + "...");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("deleting " + tableName + "...");
        }

        /* Create column families */
    
		for (String newColumnFamily : newColumnFamilies) {
            newColumnFamilyDescriptors
                    .add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(newColumnFamily)).build());
        }
        
        tableDescriptorBuilder.setColumnFamilies(newColumnFamilyDescriptors);

        System.out.println("creating " + tableName + "...");
        tableDescriptor = tableDescriptorBuilder.build();
        try {
            admin.createTable(tableDescriptor);
        } finally {
            admin.close();
        }
    }
};