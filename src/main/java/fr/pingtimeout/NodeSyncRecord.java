package fr.pingtimeout;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

/*
 keyspace_name      | table_name      | range_group | start_token          | end_token            | last_successful_validation                                                                              | last_unsuccessful_validation                                                                            | locked_by
--------------------+-----------------+-------------+----------------------+----------------------+---------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------+-----------
       dse_security |   digest_tokens |        0xdd |  6709070199063470667 |  6735847142466236680 | {started_at: '2020-10-15 01:38:29.591000+0000', outcome: 0, missing_nodes: null, was_incremental: True} |                                                                                                    null |      null
       dse_security |   digest_tokens |        0xdd |  6735847142466236680 |  6757561564810904124 | {started_at: '2020-10-15 01:39:00.216000+0000', outcome: 0, missing_nodes: null, was_incremental: True} |                                                                                                    null |      null
 system_distributed | nodesync_status |        0x6d | -1368668084254826108 | -1357170539642295314 | {started_at: '2020-10-15 05:50:46.362000+0000', outcome: 0, missing_nodes: null, was_incremental: True} | {started_at: '2020-10-09 14:58:52.753000+0000', outcome: 4, missing_nodes: null, was_incremental: True} |      null
 */

public class NodeSyncRecord implements Comparable<NodeSyncRecord>
{
    private static final Comparator<NodeSyncRecord> TOKEN_RANGE_AND_TIME_COMPARATOR = Comparator
        .comparing(NodeSyncRecord::getTokenRange)
        .thenComparing(NodeSyncRecord::getLastValidation);

    private String keyspace, table;
    private TokenRange tokenRange;
    private Instant lastValidation;
    private Instant lastSuccess;
    private Set<InetAddress> missingNodes;
    private int lastOutcome;

    private NodeSyncRecord(String keyspace, String table, TokenRange tokenRange, Instant lastValidation,
        int lastOutcome, Instant lastSuccess, Set<InetAddress> missingNodes)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tokenRange = tokenRange;
        this.lastValidation = lastValidation;
        this.lastOutcome = lastOutcome;
        this.lastSuccess = lastSuccess;
        this.missingNodes = missingNodes;
    }

    static Stream<NodeSyncRecord> recordFromRow(Row row)
    {
        UdtValue lsvUdt = row.getUdtValue("last_successful_validation");
        Instant lsvTime = lsvUdt == null ? Instant.EPOCH : lsvUdt.getInstant("started_at");
        UdtValue luvUdt = row.getUdtValue("last_unsuccessful_validation");
        Instant luvTime = luvUdt == null ? Instant.EPOCH : luvUdt.getInstant("started_at");

        UdtValue lastValidationUdt = lsvTime.isAfter(luvTime) ? lsvUdt : luvUdt;
        Instant lastValidationTime = lastValidationUdt.getInstant("started_at");
        Set<InetAddress> missingNodes = lastValidationUdt.getSet("missing_nodes", InetAddress.class);
        byte lastOutcome = lastValidationUdt.getByte("outcome");

        long start_token = row.getLong("start_token");
        long end_token = row.getLong("end_token");

        if (start_token > end_token)
        {
            // Two token ranges: the first one is from start_token to Long.MAX_VALUE, the second one is from
            // Long.MIN_VALUE to end_token
            return Stream.of(
                new NodeSyncRecord(
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    new TokenRange(start_token, Long.MAX_VALUE),
                    lastValidationTime,
                    lastOutcome,
                    lsvTime,
                    missingNodes),
                new NodeSyncRecord(
                    row.getString("keyspace_name"),
                    row.getString("table_name"),
                    new TokenRange(Long.MIN_VALUE, end_token),
                    lastValidationTime,
                    lastOutcome,
                    lsvTime,
                    missingNodes)
            );
        }
        else
        {
            return Stream.of(new NodeSyncRecord(
                row.getString("keyspace_name"),
                row.getString("table_name"),
                new TokenRange(start_token, end_token),
                lastValidationTime,
                lastOutcome,
                lsvTime,
                missingNodes));
        }
    }

    static NodeSyncRecord createFullTokenRangeUncompletedRecord(String keyspace, String table)
    {
        return new NodeSyncRecord(
            keyspace, table,
            TokenRange.FULL_TOKEN_RANGE,
            Instant.EPOCH, 4, Instant.EPOCH,
            Collections.emptySet());

    }

    NodeSyncRecord mergeWith(NodeSyncRecord that)
    {
        return new NodeSyncRecord(
            this.keyspace,
            this.table,
            this.tokenRange.mergeWith(that.tokenRange),
            // Keep the least recent validation attempt
            this.lastValidation.compareTo(that.lastSuccess) < 0 ? this.lastValidation : that.lastValidation,
            Math.max(this.lastOutcome, that.lastOutcome),
            // Keep the most recent success
            this.lastSuccess.compareTo(that.lastSuccess) > 0 ? this.lastSuccess : that.lastSuccess,
            this.lastSuccess.compareTo(that.lastSuccess) > 0 ? this.missingNodes : that.missingNodes
        );
    }

    NodeSyncRecord withLowerBound(long lowerBound)
    {
        return new NodeSyncRecord(
            this.keyspace,
            this.table,
            this.tokenRange.withLowerBound(lowerBound),
            this.lastValidation,
            this.lastOutcome,
            this.lastSuccess,
            this.missingNodes
        );
    }

    NodeSyncRecord withUpperBound(long upperBound)
    {
        return new NodeSyncRecord(
            this.keyspace,
            this.table,
            this.tokenRange.withUpperBound(upperBound),
            this.lastValidation,
            this.lastOutcome,
            this.lastSuccess,
            this.missingNodes
        );
    }

    public TokenRange getTokenRange()
    {
        return tokenRange;
    }

    public Instant getLastValidation()
    {
        return lastValidation;
    }

    public int getLastOutcome()
    {
        return lastOutcome;
    }

    private String lastOutcomeToString()
    {
        switch (lastOutcome)
        {
            case 0:
                return "0 (fully in sync)";
            case 1:
                return "1 (fully repaired)";
            case 2:
                return "2 (partially in sync)";
            case 3:
                return "3 (partially repaired)";
            case 4:
                return "4 (validation uncompleted)";
            case 5:
                return "5 (failed)";
            default:
                return "invalid outcome code (" + lastOutcome + ")";
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s, range %s, lastOutcome=%s",
            keyspace,
            table,
            tokenRange,
            lastOutcomeToString());
    }

    @Override
    public int compareTo(final NodeSyncRecord that)
    {
        return TOKEN_RANGE_AND_TIME_COMPARATOR.compare(this, that);
    }
}