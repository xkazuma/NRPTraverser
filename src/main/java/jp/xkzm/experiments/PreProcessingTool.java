package jp.xkzm.experiments;

import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreProcessingTool {

    private static final Logger logger = LoggerFactory.getLogger(PreProcessingTool.class);

    /**
     *
     * @param neo4j
     * @param hdnLabel
     * @param relType
     * @param hdnRatio
     */
    public static void createHDNLabels(
            GraphDatabaseService neo4j,
            String               hdnLabel,
            String               relType,
            double               hdnRatio
    ) {
        /*
         * removing nodes labeled "HDN"
         */
        logger.info("Removing labels named 'HDN'.");
        try (Transaction tx = neo4j.beginTx()) {

            removeHDNLabel(tx, hdnLabel);

        }

        /*
         * labeling nodes labeled "HDN"
         */
        logger.info("Re-labelling HDNs.");
        try (Transaction tx = neo4j.beginTx()) {

            putHDNLabel(tx, hdnLabel, relType, hdnRatio);

        }

    }

    /**
     *
     * @param tx
     * @param hdnLabel
     */
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


        String match3  = String.format("MATCH (n:Person:%s) ", hdnLabel);
        String return2 = "RETURN COUNT(*) AS cnt;";
        Result hdnNum  = tx.execute(match3 + return2);

        logger.info(String.format("#HDN: %d", (Long) hdnNum.next().get("cnt")));

        tx.commit();

    }
}
