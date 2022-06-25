package jp.xkzm;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

class NHopTraverser {

    private static final Logger logger = LoggerFactory.getLogger(NHopTraverser.class);

    private final static int ARG_NUM = 12;

    private static File   dbDir;
    private static File   confFile;
    private static File   logFile;
    private static String execModeStr;
    private static String minHopStr;
    private static String maxHopStr;
    private static String relType;
    private static String targetLabel;
    private static String hdnLabel;
    private static String hdnRatioStr;
    private static String isCompressedStr;
    private static String byCypherStr;

    public static void main(String... args) {

        parseArgs(args);

        int     minhop       = Integer.parseInt(minHopStr);
        int     hop          = Integer.parseInt(maxHopStr);
        double  hdnRatio     = Double.parseDouble(hdnRatioStr);
        boolean isCompressed = Boolean.parseBoolean(isCompressedStr);
        boolean byCypher     = Boolean.parseBoolean(byCypherStr);
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(dbDir)
                .loadPropertiesFromFile(confFile.getAbsolutePath())
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

            for (int i = minhop; i <= hop; i++) {


                long startAtNHop = System.nanoTime();

                boolean isContinued = true;
                if (isCompressed) isContinued = traverseWithoutHDN(tx, hdnLabel, relType, i, byCypher);
                else              traverse(tx, hdnLabel, relType, i);

                long endAtNHop = System.nanoTime();

                logger.info(String.format(
                        "Consumed time at %d-hop [msec.]: %f",
                        i,
                        (endAtNHop - startAtNHop) / 1000.0 / 1000.
                ));

                if (! isContinued) break;

            }

        }

    }

    private static void traverse(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         n
    ) {

        String match   = String.format(
                "MATCH (n:%s)-[:%s*%d]->(m) ",
                hdnLabel,
                relType,
                n
        );
        String return_ = String.format(
                "RETURN '%d-hop' AS Nhop, '%s' in Labels(m) AS isHDN, COUNT(DISTINCT m) AS cnt;",
                n,
                hdnLabel
        );

        Result result = tx.execute(match + return_);

        while (result.hasNext()) {

            Map<String, Object> row = result.next();

            logger.info(String.format(
                    "Nhop: %s\tisHDN: %s\tcnt: %d",
                    (String)  row.get("Nhop"),
                    (Boolean) row.get("isHDN"),
                    (Long)    row.get("cnt")
            ));

        }

    }

    private static boolean traverseWithoutHDN(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         n,
            boolean     byCypher
    ) {

        if (byCypher) return traverseWithoutHDNByCypher(tx, hdnLabel, relType, n);
        else          return traverseWithoutHDNByTraversalAPI(tx, hdnLabel, relType, n);

    }

    private static boolean traverseWithoutHDNByTraversalAPI(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         n
    ) {

         Iterable<Node> nodes = tx.traversalDescription()
                .breadthFirst()
                .evaluator(Evaluators.atDepth(1))
                .relationships(RelationshipType.withName(relType))
                .traverse()
                .nodes();

        nodes.iterator().forEachRemaining(node  -> {
            logger.info(node.toString());
        });

        return false;

    }

    private static boolean traverseWithoutHDNByCypher(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         n
    ) {

        StringBuilder sb            = new StringBuilder();
        String        templateMatch = "MATCH (%s)-[:%s]->(%s) ";
        String        templateWhere = "WHERE isHDN%d = False ";
        String        templateWith  = "WITH '%s' in Labels(%s) AS isHDN%d, %s "; // hdnLabel, m + i, i, m + i
        String        queryVarM     = "m" + 1;

        if (1 < n) {

            sb.append(String.format(
                    "MATCH (n:%s)-[:%s]->(%s)",
                    hdnLabel,
                    relType,
                    queryVarM
            ));
            sb.append(String.format(
                    templateWith,
                    hdnLabel,
                    queryVarM,
                    1,
                    queryVarM
            ));

            String queryVarMNext;
            for (int i = 2; i <= n; i++) {

                queryVarMNext = "m" + i;

                sb.append(String.format(
                        templateMatch,
                        queryVarM,
                        relType,
                        queryVarMNext
                ));
                sb.append(String.format(
                        templateWhere,
                        i - 1
                ));
                sb.append(String.format(
                        templateWith,
                        hdnLabel,
                        queryVarMNext,
                        i,
                        queryVarMNext
                ));

                queryVarM = queryVarMNext;

            }

        } else if (n == 1) {

            queryVarM = "m" + 1;
            sb.append(String.format(
                    "MATCH (n:%s)-[:%s]->(%s)",
                    hdnLabel,
                    relType,
                    queryVarM
            ));

        }

        sb.append(String.format(
                "RETURN '%d-hop' AS Nhop, '%s' in Labels(%s) AS isHDN, COUNT(DISTINCT %s) AS cnt;",
                n,
                hdnLabel,
                queryVarM,
                queryVarM
        ));

        logger.debug(sb.toString());

        Result result = tx.execute(sb.toString());

        int rows = 0;
        while (result.hasNext()) {

            rows++;

            Map<String, Object> row = result.next();

            logger.info(String.format(
                    "Nhop: %s\tisHDN: %s\tcnt: %d",
                    (String) row.get("Nhop"),
                    (Boolean) row.get("isHDN"),
                    (Long) row.get("cnt")
            ));

        }

        if (rows == 0) return false;
        else           return true; //continued

    }

    private static void removeHDNLabel(Transaction tx, String hdnLabel) {

        String match  = String.format(
                "MATCH (n:%s) ",
                hdnLabel
        );
        String remove = String.format(
                "REMOVE n:%s;",
                hdnLabel
        );

        Result result = tx.execute(match + remove);

        logger.info(result.getNotifications().toString());

        tx.commit();

    }

    private static void putHDNLabel(Transaction tx, String hdnLabel, String relType, double hdnRatio) {

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

        Result result = tx.execute(match2 + with2 + orderby + limit + set);

        logger.info(result.getNotifications().toString());

        tx.commit();

    }

    private static void registerShutdownHook(final DatabaseManagementService dbms) {

        Runtime.getRuntime().addShutdownHook(new Thread(dbms::shutdown));

    }

    private static void printUsage() {

        String usage =
                "Usage: mvn exec:java -Dexec.args=\" \n" +
                        "\t'1st arg: Path to Neo4j DB dir' \n" +
                        "\t'2nd arg: Path to Neo4j conf file' \n" +
                        "\t'3rd arg: Path to log file' \n" +
                        "\t'4th arg: execution mode (ProposedMethod or Baseline1)' \n" +
                        "\t'5th arg: min hop' \n" +
                        "\t'6th arg: max hop' \n" +
                        "\t'7th arg: Target relationship' \n" +
                        "\t'8th arg: Target label for nodes' \n" +
                        "\t'9th arg: a label name for high degree nodes' \n" +
                        "\t'10th arg: HDN ratio of all nodes' \n" +
                        "\t'11th arg: boolean-typed value: true is compressed path, false is full path ' \n" +
                        "\t'12th arg: boolean-typed value: whether or not is to use cypher'\" \n";
        System.err.println(usage);
        System.exit(-1);

    }

    private static void parseArgs(String... args) {

        if (args.length != ARG_NUM) printUsage();

        dbDir           = new File(args[0]);
        // URL uri         = NHopTraverser.class.getClass().getResource(args[1]);
        // confFile        = new File(uri.toURI());
        logFile         = new File(args[2]);
        execModeStr     = args[3];
        minHopStr       = args[4];
        maxHopStr       = args[5];
        relType         = args[6];
        targetLabel     = args[7];
        hdnLabel        = args[8];
        hdnRatioStr     = args[9];
        isCompressedStr = args[10];
        byCypherStr     = args[11];

        // 1st Arg if (! dbDir.exists()) { }
        // 2nd Arg if (! confFile.exists()) { }
        try {

            URL url  = NHopTraverser.class.getClassLoader().getResource(args[1]);
            confFile = new File(url.toURI());
            if (! confFile.exists()) {

                System.err.println("The 3th argument should be a path for config file.");
                System.exit(-1);

            }

        } catch (URISyntaxException use) {

            use.printStackTrace();

        }
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

            Integer.parseInt(minHopStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 9th argument should be integer-typed value meaning to start traversing from $minhop-hop.");
            System.exit(-1);

        }

        // 10th Arg
        try {

            Integer.parseInt(maxHopStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 10th argument should be integer-typed value meaning to end traversing at $maxhop-hop.");
            System.exit(-1);

        }

        // 10th Arg
        try {

            Boolean.parseBoolean(isCompressedStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 11th argument should be boolean-typed value meaning whether or not HDN-based compression is enabled.");
            System.exit(-1);

        }

        // 12th Arg
        try {

            Boolean.parseBoolean(byCypherStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 12th argument should be boolean-typed value meaning whether or not queries use Cypher.");
            System.exit(-1);

        }

    }

}

