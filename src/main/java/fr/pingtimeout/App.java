package fr.pingtimeout;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;

public class App
{
    public static void main(String[] args)
    {
        String host = args.length < 1 ? "localhost" : args[0];
        int port = args.length < 2 ? 9042 : Integer.parseInt(args[1]);
        String dc = args.length < 3 ? "DC1" : args[2];
        String keyspace = "domain_1300";
        List<String> tables = Arrays
            .asList("xml_doc_1300", "xml_doc_1305", "xml_doc_1307", "xml_idx_1300_1", "xml_idx_1300_2",
                "xml_idx_1300_3", "xml_idx_1300_4", "xml_idx_1300_5", "xml_idx_1301_1", "xml_idx_1305_1");
//        tables = Arrays.asList("xml_idx_1305_1");

        try (CqlSession session = connectToNode(host, port, dc))
        {
            for (String table : tables)
            {
                System.out.printf("Checking %s.%s...%n", keyspace, table);
                processTable(keyspace, session, table).forEach(System.out::println);
            }
        }
    }

    private static CqlSession connectToNode(String host, int port, String localDc)
    {
        System.out.printf("Connecting to %s:%d and using %s as local Datacenter%n", host, port, localDc);
        return CqlSession.builder()
            .addContactPoint(new InetSocketAddress(host, port))
            .withLocalDatacenter(localDc)
            .build();
    }

    private static TreeSet<NodeSyncRecord> processTable(String keyspace, CqlSession session, String table)
    {
        ResultSet resultSet = session.execute("" +
            "SELECT * " +
            "FROM system_distributed.nodesync_status " +
            "WHERE keyspace_name = '" + keyspace + "' " +
            "AND table_name = '" + table + "' " +
            "ALLOW FILTERING");
        TreeSet<NodeSyncRecord> nodeSyncRecords = StreamSupport
            .stream(resultSet.spliterator(), false)
            .flatMap(NodeSyncRecord::recordFromRow)
            .collect(Collectors.toCollection(TreeSet::new));

        TreeSet<NodeSyncRecord> tokenRangeValidationState = new TreeSet<>();
        tokenRangeValidationState.add(NodeSyncRecord.createFullTokenRangeUncompletedRecord(keyspace, table));
        for (NodeSyncRecord record : nodeSyncRecords)
        {
            NodeSyncRecord highestRecord = tokenRangeValidationState.last();
            //System.out.println("Highest record : " + highestRecord);
            //System.out.println("Current record : " + record);
            if (highestRecord.getTokenRange().intersectsWith(record.getTokenRange()))
            {
                if(highestRecord.getLastOutcome() == record.getLastOutcome()) {
                    //System.out.println("Intersect and match outcome");
                    tokenRangeValidationState.remove(highestRecord);
                    tokenRangeValidationState.add(highestRecord.mergeWith(record));
                } else {
                    if(highestRecord.getTokenRange().getLowerBound() == record.getTokenRange().getLowerBound()) {
                        //System.out.println("Intersect with different outcome starting at the same token");
                        tokenRangeValidationState.remove(highestRecord);
                        tokenRangeValidationState.add(record);
                    } else {
                        //System.out.println("Intersect with different outcome");
                        tokenRangeValidationState.remove(highestRecord);
                        tokenRangeValidationState.add(highestRecord.withUpperBound(record.getTokenRange().getLowerBound()));
                        tokenRangeValidationState.add(record);
                    }
                }
            }
            else
            {
                //System.out.println("Does not intersect");
                // Gap in token ranges
                tokenRangeValidationState.remove(highestRecord);
                tokenRangeValidationState.add(highestRecord.withUpperBound(record.getTokenRange().getLowerBound()));
                tokenRangeValidationState.add(record);
            }
            //System.out.println();
        }
        return tokenRangeValidationState;
    }
}
