package jp.xkzm;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import java.io.File;

class NhopTraverser {

    private final static int ARG_NUM = 9;

    private static File   dbDir;
    private static File   confFile;
    private static File   logFile;
    private static String execModeStr;
    private static String maxHopStr;
    private static String relType;
    private static String targetLabel;
    private static String hdnLabel;
    private static String hdnRatioStr;

    public static void main(String... args) {

        parseArgs(args);

        int    hop      = Integer.parseInt(maxHopStr);
        double hdnRatio = Double.parseDouble(hdnRatioStr);

        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(dbDir)
                .setConfig(GraphDatabaseSettings.pagecache_memory,                   "1G")
                .setConfig(GraphDatabaseSettings.pagecache_warmup_enabled,           false)
                .setConfig(GraphDatabaseSettings.tx_state_off_heap_block_cache_size, 16)
                .setConfig(GraphDatabaseSettings.force_small_id_cache,               false)
                .setConfig(GraphDatabaseSettings.query_cache_size,                   16)
                .setConfig(GraphDatabaseSettings.pagecache_warmup_prefetch,          false)
                .build();
        registerShutdownHook(dbms);

        GraphDatabaseService neo4j = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);


        try (Transaction tx = neo4j.beginTx()) {

            removeHDNLabel(tx, hdnLabel);

        }

        try (Transaction tx = neo4j.beginTx()) {

            putHDNLabel(tx, hdnLabel, relType, hdnRatio);

        }

        try (Transaction tx = neo4j.beginTx()) {

            for (int i = 1; i <= hop; i++) {

                traverse(tx, hdnLabel, relType, hop);

            }

        }

        Runtime r = Runtime.getRuntime();

    }

    private static void traverse(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         n
    ) {

        try (tx) {

            String match   = String.format(
                    "MATCH (n:%s)-[:%s*%d]->(m) ",
                    hdnLabel,
                    relType,
                    n
            );
            String return_ = String.format(
                    "RETURN '%s' in Labels(m) AS isHDN, COUNT(*) AS cnt;",
                    hdnLabel
            );

            tx.execute(match + return_);

            tx.commit();

        }

    }

    private static void removeHDNLabel(Transaction tx, String hdnLabel) {

        try (tx) {

            String match  = String.format(
                    "MATCH (n:%s) ",
                    hdnLabel
            );
            String remove = String.format(
                    "REMOVE n:%s;",
                    hdnLabel
            );

            tx.execute(match + remove);

            tx.commit();

        }

    }

    private static void putHDNLabel(Transaction tx, String hdnLabel, String relType, double hdnRatio) {

        try (tx) {

            String match1    = "MATCH (n:Person) ";
            String return1   = "RETURN COUNT(*) AS cnt;";
            Result personNum = tx.execute(match1 + return1);

            String match2  = "MATCH (n:Person) ";
            String with2   = String.format(
                    "WITH SIZE((n)-[:%s]->()) AS degree, n ",
                    relType
            );
            String orderby = "ORDER BY degree DESC ";
            int limitNum = (int) ((Long) personNum.next().get("cnt") * hdnRatio);
            String limit   = String.format(
                    "LIMIT %d ",
                    limitNum
            );
            String set     = String.format(
                    "SET n:%s;",
                    hdnLabel
            );

            tx.execute(match2 + with2 + orderby + limit + set);

            tx.commit();

        }

    }

    private static void registerShutdownHook(final DatabaseManagementService dbms) {

        Runtime.getRuntime().addShutdownHook(new Thread(dbms::shutdown));

    }

    private static void printUsage() {

        String usage =
                "Usage: mvn exec:java -Dexec.args=\"" +
                        "'1st arg: Path to Neo4j DB dir' " +
                        "'2nd arg: Path to Neo4j conf file' " +
                        "'3rd arg: Path to log file' " +
                        "'4th arg: execution mode (ProposedMethod or Baseline1)' " +
                        "'5th arg: Max hop' " +
                        "'6th arg: Target relationship' " +
                        "'7th arg: Target node's label' ";
        System.err.println(usage);
        System.exit(-1);

    }

    private static void parseArgs(String... args) {

        if (args.length != ARG_NUM) printUsage();

        dbDir       = new File(args[0]);
        confFile    = new File(args[1]);
        logFile     = new File(args[2]);
        execModeStr = args[3];
        maxHopStr   = args[4];
        relType     = args[5];
        targetLabel = args[6];
        hdnLabel    = args[7];
        hdnRatioStr = args[8];

        // 1st Arg if (! dbDir.exists()) { }
        // 2nd Arg if (! confFile.exists()) { }
        // 3rd Arg if (! logFile.exists()) { }
        // 4th Arg
        switch (execModeStr) {
            case "ProposedMethod":
            case "Baseline1": break;
            default:
                System.err.println("The 4th argument should be named: ProposedMethod or Baseline1.");
                System.exit(-1);
        }

        // 5th Arg
        try {

            Integer.parseInt(maxHopStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 5th argument should be integer value for Hops.");
            System.exit(-1);

        }

        // 6th Arg
        switch (relType) {

            case "KNOWS":
            case "REPLY_TO": break;
            default:
                System.err.println("The 6th argument should be a targeted relationship: KNOWS or REPLY_TO.");
                System.exit(-1);

        }

        // 7th Arg
        switch (targetLabel) {

            case "Person":
            case "Message": break;
            default:
                System.err.println("The 7th argument should be a targeted label: Person or Message.");
                System.exit(-1);

        }

        // 8th Arg
        switch (hdnLabel) {

            case "HDN20":
            case "HDN10":
            case "HDN05":
            case "HDN01": break;
            default:
                System.err.println("The 8th argument should be a name of HDNs: (HDN20 | HDN10 | HDN05 | HDN01).");
                System.exit(-1);

        }

        // 9th Arg
        try {

            Double.parseDouble(maxHopStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 9th argument should be double-typed value meaning HDN ratio.");
            System.exit(-1);

        }

    }

}

