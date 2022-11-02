package jp.xkzm.experiments;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class PrepareTool {

    private static final Logger logger = LoggerFactory.getLogger(PrepareTool.class);

    /**
     * initializing Neo4j
     * @param dbDir
     * @param confFile
     * @return
     */
    public static GraphDatabaseService initializingNeo4j(
            File dbDir,
            File confFile
    ) {

        logger.info("Initializing Neo4j database.");
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(dbDir)
                .loadPropertiesFromFile(confFile.getAbsolutePath())
                .build();
        registerShutdownHook(dbms);

        GraphDatabaseService neo4j = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        return neo4j;

    }

    /**
     * Automatic shutdown
     * @param dbms
     */
    private static void registerShutdownHook(final DatabaseManagementService dbms) {

        Runtime.getRuntime().addShutdownHook(new Thread(dbms::shutdown));

    }

}

