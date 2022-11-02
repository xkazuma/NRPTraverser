package jp.xkzm.experiments;

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
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class NHopTraverser {

    private static final Logger logger = LoggerFactory.getLogger(NHopTraverser.class);

    private final static int ARG_NUM = 14;

    private static File   dbDir;
    private static File   confFile;
    private static File   logFile;
    private static String approachStr;
    private static String execModeStr;
    private static String minHopStr;
    private static String maxHopStr;
    private static String relType;
    private static String targetLabel;
    private static String hdnLabel;
    private static String hdnRatioStr;
    private static String isCompressedStr;
    private static String byCypherStr;
    private static String demandPathStr;

    public static void main(String... args) {

        /*
         * parsing parameter
         */
        logger.info("Parsing parameters.");
        parseArgs(args);

        int     minHop       = Integer.parseInt(minHopStr);
        int     maxHop       = Integer.parseInt(maxHopStr);
        double  hdnRatio     = Double.parseDouble(hdnRatioStr);
        boolean isCompressed = Boolean.parseBoolean(isCompressedStr);
        boolean byCypher     = Boolean.parseBoolean(byCypherStr);
        boolean demandPath   = Boolean.parseBoolean(demandPathStr);

        GraphDatabaseService neo4j = PrepareTool.initializingNeo4j(dbDir, confFile);

        PreProcessingTool.createHDNLabels(neo4j, hdnLabel, relType, hdnRatio);

        /*
         * mode:
         *  - HDN-START: repeat traversal from all HDN nodes
         */
        if (execModeStr.equals("HDN-START")) {

            logger.info("MODE: HDN-START");

            if (approachStr.equals("AdjacencyList")) {

                logger.info("METHOD: AdjacencyList");
                logger.info("Start traversing by adjacency lists.");

                try (Transaction tx = neo4j.beginTx()) {

                    traverseFull(tx, hdnLabel, relType, minHop, maxHop, byCypher, demandPath);

                }



            } else if (approachStr.equals("RPindex")) {

                logger.info("Start traversing by RP-index.");


            } else if (approachStr.equals("RPindex-rso")) {

                logger.info("Start traversing by RP-index with RSO.");


            } else if (approachStr.equals("RPDindex")) {

                logger.info("Start traversing by RPD-index.");


            } else if (approachStr.equals("RPDindex-rso")) {

                logger.info("Start traversing by RPD-index with RSO.");

            }

        }

    }

    private static void traverseFull(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         minHop,
            int         maxHop,
            boolean     byCypher,
            boolean     demandPath
    ) {

        if (byCypher) traverseFullByCypher(tx, hdnLabel, relType, maxHop);
        else          traverseFullByTraversalAPI(tx, hdnLabel, relType, minHop, maxHop, demandPath);

    }

    private static void traverseFullByCypher(
        Transaction tx,
        String      hdnLabel,
        String      relType,
        int         maxHop
    ) {

        String match   = String.format(
                "MATCH (n:%s)-[:%s*%d]->(m) ",
                hdnLabel,
                relType,
                maxHop
        );
        String return_ = String.format(
                "RETURN '%d-hop' AS Nhop, '%s' in Labels(m) AS isHDN, COUNT(DISTINCT m) AS cnt;",
                maxHop,
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

    private static void traverseFullByTraversalAPI(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         minHop,
            int         maxHop,
            boolean     demandPath

    ) {

        // counting HDN nodes
        String match  = String.format("MATCH (n:Person:%s) ", hdnLabel);
        String return_ = "RETURN COUNT(*) AS cnt;";
        Result hdnNum  = tx.execute(match + return_);
        long hdnN = (Long) hdnNum.next().get("cnt");

        // for each HDN
        int hdnCnt = 0;
        for (Iterator<Node> it = tx.findNodes(Label.label(hdnLabel)).stream().iterator(); it.hasNext(); ) {

            logger.info(String.format("HDN %d / %d", ++hdnCnt, hdnN));

            Node hdn = it.next();

            int outDegree  = hdn.getDegree(Direction.OUTGOING);
            int inDegree   = hdn.getDegree(Direction.INCOMING);
            int bothDegree = hdn.getDegree(Direction.BOTH);

            long startAtNHop;
            long endAtNHop;
            long count;
            for (int i = minHop; i <= maxHop; i++) {

                startAtNHop = System.nanoTime();
                if (demandPath) { // demand for path

                    Iterator<Path> paths = tx.traversalDescription()
                            // .breadthFirst()
                            .evaluator(Evaluators.atDepth(i))
                            .relationships(RelationshipType.withName(relType), Direction.OUTGOING)
                            .traverse(hdn)
                            .iterator();


                    // Iterator to Stream
                    Spliterator<Path> spliterator = Spliterators.spliteratorUnknownSize(paths, 0);
                    Stream<Path>      stream      = StreamSupport.stream(spliterator, true);
                    count = stream.count();

                } else { // demand for reachable nodes

                    Iterable<Node> nodes = tx.traversalDescription()
                            // .breadthFirst()
                            .evaluator(Evaluators.atDepth(i))
                            .relationships(RelationshipType.withName(relType), Direction.OUTGOING)
                            .traverse(hdn)
                            .nodes();

                    // Iterator to Stream
                    Spliterator<Node> spliterator = Spliterators.spliteratorUnknownSize(nodes.iterator(), 0);
                    Stream<Node> stream = StreamSupport.stream(spliterator, true);
                    count = stream.count();

                }

                endAtNHop = System.nanoTime();

                logger.info(String.format(
                        "HDNID: %d\tIN: %d\tOUT: %d\tBOTH: %d\tNHOP: %d\tCNT: %d\tTIME[msec.]: %f",
                        hdn.getId(),
                        inDegree,
                        outDegree,
                        bothDegree,
                        i,
                        count,
                        (endAtNHop - startAtNHop) / 1000.0 / 1000.0
                ));

            }

        }

    }

    private static void traverseWithoutHDN(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         minHop,
            int         maxHop,
            boolean     byCypher
    ) {

        if (byCypher) traverseWithoutHDNByCypher(tx, hdnLabel, relType, minHop, maxHop);
        else          traverseWithoutHDNByTraversalAPI(tx, hdnLabel, relType, minHop, maxHop);

    }

    private static void traverseWithoutHDNByTraversalAPI(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         minHop,
            int         maxHop
    ) {

        Set<Long> skip = new ConcurrentSkipListSet<>();
        Set<Long> next = new ConcurrentSkipListSet<>();
        Set<Long> curr = new ConcurrentSkipListSet<>();
        for (int i = minHop; i <= maxHop; i++) {

            long startAtNHop = System.nanoTime();

            if (i == minHop) {

                tx.findNodes(Label.label(hdnLabel)).forEachRemaining(hdn -> {

                    tx.traversalDescription()
                            .breadthFirst()
                            .evaluator(Evaluators.atDepth(1))
                            .relationships(RelationshipType.withName(relType), Direction.OUTGOING)
                            .traverse(hdn)
                            .nodes()
                            .iterator()
                            .forEachRemaining(node -> {

                                if (node.hasLabel(Label.label(hdnLabel))) skip.add(node.getId());
                                else                                      next.add(node.getId());

                            });

                });

            } else {

                if (next.size() == 0) break;

                skip.clear();
                curr = Set.copyOf(next);
                next.clear();

                curr.iterator().forEachRemaining(v -> {

                    tx.traversalDescription()
                            .breadthFirst()
                            .evaluator(Evaluators.atDepth(1))
                            .relationships(RelationshipType.withName(relType), Direction.OUTGOING)
                            .traverse(tx.getNodeById(v))
                            .nodes()
                            .iterator()
                            .forEachRemaining(node -> {

                                if (node.hasLabel(Label.label(hdnLabel))) skip.add(node.getId());
                                else                                      next.add(node.getId());

                            });

                });

            }

            long endAtNHop = System.nanoTime();

            logger.info(String.format(
                    "Nhop: %d\tisHDN: %s\tcnt: %d",
                    i,
                    "true",
                    skip.size()
            ));

            logger.info(String.format(
                    "Nhop: %d\tisHDN: %s\tcnt: %d",
                    i,
                    "false",
                    next.size()
            ));

            logger.info(String.format(
                    "Consumed time at %d-hop [msec.]: %f",
                    i,
                    (endAtNHop - startAtNHop) / 1000.0 / 1000.
            ));

        }

    }

    private static void traverseWithoutHDNByCypher(
            Transaction tx,
            String      hdnLabel,
            String      relType,
            int         minHop,
            int         maxHop
    ) {

        StringBuilder sb            = new StringBuilder();
        String        templateMatch = "MATCH (%s)-[:%s]->(%s) ";
        String        templateWhere = "WHERE isHDN%d = False ";
        String        templateWith  = "WITH '%s' in Labels(%s) AS isHDN%d, %s "; // hdnLabel, m + i, i, m + i
        String        queryVarM     = "m" + 1;

        for(int i = minHop; i <= maxHop; i++) {

            if (1 < i) {

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
                for (int j = 2; j <= i; j++) {

                    queryVarMNext = "m" + j;

                    sb.append(String.format(
                            templateMatch,
                            queryVarM,
                            relType,
                            queryVarMNext
                    ));
                    sb.append(String.format(
                            templateWhere,
                            j - 1
                    ));
                    sb.append(String.format(
                            templateWith,
                            hdnLabel,
                            queryVarMNext,
                            j,
                            queryVarMNext
                    ));

                    queryVarM = queryVarMNext;

                }

            } else if (i == 1) {

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
                    i,
                    hdnLabel,
                    queryVarM,
                    queryVarM
            ));

            logger.debug(sb.toString());

            Result result = tx.execute(sb.toString());

            while (result.hasNext()) {

                Map<String, Object> row = result.next();

                logger.info(String.format(
                        "Nhop: %s\tisHDN: %s\tcnt: %d",
                        (String) row.get("Nhop"),
                        (Boolean) row.get("isHDN"),
                        (Long) row.get("cnt")
                ));

            }

        }

    }



    private static void printUsage() {

        String usage =
                "Usage: mvn exec:java -Dexec.args=\" \n" +
                        "\t'1st arg: Path to Neo4j DB dir' \n" +
                        "\t'2nd arg: Path to Neo4j conf file' \n" +
                        "\t'3rd arg: Path to log file' \n" +
                        "\t'4th arg: approach ('AdjacencyList', 'RPindex', 'RPindex-rso', 'RPDindex', 'RPDindex-rso') \n" +
                        "\t'5th arg: execution mode ('HDN-START', 'INDEX-CONSTRUCTION') \n" +
                        "\t'6th arg: min hop' \n" +
                        "\t'7th arg: max hop' \n" +
                        "\t'8th arg: Target relationship' \n" +
                        "\t'9th arg: Target label for nodes' \n" +
                        "\t'10th arg: a label name for high degree nodes' \n" +
                        "\t'11th arg: HDN ratio of all nodes' \n" +
                        "\t'12th arg: boolean-typed value: true is compressed path, false is full path ' \n" +
                        "\t'13th arg: boolean-typed value: whether or not is to use cypher'\" \n" +
                        "\t'14th arg: boolean-typed value: whether path demand or node one'\" \n";
        System.err.println(usage);
        System.exit(-1);

    }

    private static void parseArgs(String... args) {

        if (args.length != ARG_NUM) printUsage();

        dbDir           = new File(args[0]);
        // URL uri         = NHopTraverser.class.getClass().getResource(args[1]);
        // confFile        = new File(uri.toURI());
        logFile         = new File(args[2]);
        approachStr     = args[3];
        execModeStr     = args[4];
        minHopStr       = args[5];
        maxHopStr       = args[6];
        relType         = args[7];
        targetLabel     = args[8];
        hdnLabel        = args[9];
        hdnRatioStr     = args[10];
        isCompressedStr = args[11];
        byCypherStr     = args[12];
        demandPathStr   = args[13];

        // 1st Arg if (! dbDir.exists()) { }
        // 2nd Arg if (! confFile.exists()) { }
        try {

            URL url  = NHopTraverser.class.getClassLoader().getResource(args[1]);
            confFile = new File(url.toURI());
            if (! confFile.exists()) {

                System.err.println("The 2nd argument should be a path for config file.");
                System.exit(-1);

            }

        } catch (URISyntaxException use) {

            use.printStackTrace();

        }
        // 3rd Arg if (! logFile.exists()) { }
        // 4th Arg
        switch (approachStr) {
            case "AdjacencyList":
            case "RPindex":
            case "RPindex-rso":
            case "RPDindex":
            case "RPDindex-rso":
            case "ProposedMethod":
            case "Baseline1": break;
            default:
                System.err.println("The 4th argument should be named: ProposedMethod or Baseline1.");
                System.exit(-1);
        }

        // 5th Arg
        switch (execModeStr) {
            case "HDN-START":
            case "INDEX-CONSTRUCTION": break;
            default:
                System.err.println("The 5th argument should be named: HDN-START / INDEX-CONSTRUCTION.");
                System.exit(-1);
        }

        // 6th Arg
        try {

            Integer.parseInt(maxHopStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 6th argument should be integer value for Hops.");
            System.exit(-1);

        }

        // 7th Arg
        switch (relType) {

            case "KNOWS":
            case "REPLY_TO": break;
            default:
                System.err.println("The 7th argument should be a targeted relationship: KNOWS or REPLY_TO.");
                System.exit(-1);

        }

        // 8th Arg
        switch (targetLabel) {

            case "Person":
            case "Message": break;
            default:
                System.err.println("The 8th argument should be a targeted label: Person or Message.");
                System.exit(-1);

        }

        // 9th Arg
        switch (hdnLabel) {

            case "HDN20":
            case "HDN10":
            case "HDN05":
            case "HDN01": break;
            default:
                System.err.println("The 9th argument should be a name of HDNs: (HDN20 | HDN10 | HDN05 | HDN01).");
                System.exit(-1);

        }

        // 10th Arg
        try {

            Integer.parseInt(minHopStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 10th argument should be integer-typed value meaning to start traversing from $minhop-hop.");
            System.exit(-1);

        }

        // 11th Arg
        try {

            Integer.parseInt(maxHopStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 11th argument should be integer-typed value meaning to end traversing at $maxhop-hop.");
            System.exit(-1);

        }

        // 12th Arg
        try {

            Boolean.parseBoolean(isCompressedStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 12th argument should be boolean-typed value meaning whether or not HDN-based compression is enabled.");
            System.exit(-1);

        }

        // 13th Arg
        try {

            Boolean.parseBoolean(byCypherStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 13th argument should be boolean-typed value meaning whether or not queries use Cypher.");
            System.exit(-1);

        }

        // 14th Arg
        try {

            Boolean.parseBoolean(demandPathStr);

        } catch (NumberFormatException nfe) {

            System.err.println("The 14th argument should be boolean-typed value meaning whether paths demand or reachable nodes one.");
            System.exit(-1);

        }

    }

}

