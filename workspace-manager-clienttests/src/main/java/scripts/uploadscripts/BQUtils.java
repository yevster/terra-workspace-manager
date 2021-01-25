package scripts.uploadscripts;

import bio.terra.testrunner.common.utils.AuthenticationUtils;
import bio.terra.testrunner.common.utils.BigQueryUtils;
import bio.terra.testrunner.runner.config.ServiceAccountSpecification;
import bio.terra.testrunner.runner.config.TestUserSpecification;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQueryOptions.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class BQUtils {
    private static final Logger logger = LoggerFactory.getLogger(BQUtils.class);
    public static final List<String> bigQueryScope = Collections.unmodifiableList(Arrays.asList(
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/devstorage.full_control"));

    private BQUtils() {
    }

    public static BigQuery getClientForTestUser(TestUserSpecification testUser, String googleProjectId) throws IOException {
        logger.debug("Fetching credentials and building BigQuery client object for test user: {}", testUser.name);
        List<String> scopes = bigQueryScope;
        scopes.addAll(AuthenticationUtils.userLoginScopes);
        GoogleCredentials userCredential = AuthenticationUtils.getDelegatedUserCredential(testUser, scopes);
        BigQuery bigQuery = (BigQuery)((Builder)((Builder)BigQueryOptions.newBuilder().setProjectId(googleProjectId)).setCredentials(userCredential)).build().getService();
        return bigQuery;
    }

    public static BigQuery getClientForServiceAccount(ServiceAccountSpecification serviceAccount, String googleProjectId) throws IOException {
        logger.debug("Fetching credentials and building BigQuery client object for service account: {}", serviceAccount.name);
        GoogleCredentials serviceAccountCredentials = AuthenticationUtils.getServiceAccountCredential(serviceAccount, bigQueryScope);
        System.out.println(serviceAccountCredentials.getAccessToken().getTokenValue());
        serviceAccountCredentials.refresh();
        System.out.println(serviceAccountCredentials.getAccessToken().getTokenValue());
        BigQuery bigQuery = (BigQuery)((Builder)((Builder)BigQueryOptions.newBuilder().setProjectId(googleProjectId)).setCredentials(serviceAccountCredentials)).build().getService();
        return bigQuery;
    }

    public static TableResult queryBigQuery(BigQuery bigQueryClient, String query) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
        return bigQueryClient.query(queryConfig, new JobOption[0]);
    }

    public static boolean checkRowExists(BigQuery bigQueryClient, String projectId, String datasetName, String tableName, String fieldName, String fieldValue) throws InterruptedException {
        String tableRef = String.format("`%s.%s.%s`", projectId, datasetName, tableName);
        String queryForRow = String.format("SELECT 1 FROM %s WHERE %s = '%s' LIMIT %s", tableRef, fieldName, fieldValue, 1);
        logger.debug("queryForRow: {}", queryForRow);
        TableResult result = queryBigQuery(bigQueryClient, queryForRow);
        AtomicInteger numMatchedRows = new AtomicInteger();
        result.iterateAll().forEach((r) -> {
            numMatchedRows.getAndIncrement();
        });
        if (numMatchedRows.get() > 0) {
            logger.debug("Row already exists: {} = {}, Matches found: {}", new Object[]{fieldName, fieldValue, numMatchedRows.get()});
        }

        return numMatchedRows.get() > 0;
    }

    public static void insertAllIntoBigQuery(BigQuery bigQueryClient, InsertAllRequest request) {
        InsertAllResponse response = bigQueryClient.insertAll(request);
        if (response.hasErrors()) {
            logger.error("hasErrors is true after inserting into the {}.{}.{} table", new Object[]{((BigQueryOptions)bigQueryClient.getOptions()).getProjectId(), request.getTable().getDataset(), request.getTable().getTable()});
            Iterator var3 = response.getInsertErrors().entrySet().iterator();

            while(var3.hasNext()) {
                Map.Entry<Long, List<BigQueryError>> entry = (Map.Entry)var3.next();
                ((List)entry.getValue()).forEach((bqe) -> {
                    logger.info("bqerror: {}", bqe.toString());
                });
            }
        }

        logger.info("Successfully inserted to BigQuery table: {}.{}.{}", new Object[]{((BigQueryOptions)bigQueryClient.getOptions()).getProjectId(), request.getTable().getDataset(), request.getTable().getTable()});
    }

    public static String getDatasetName(String datasetName) {
        return "datarepo_" + datasetName;
    }

    public static String buildSelectQuery(String project, String datasetName, String tableName, String select, Long limit) {
        String tableRef = String.format("`%s.%s.%s`", project, datasetName, tableName);
        String sqlQuery = String.format("SELECT %s FROM %s LIMIT %s", select, tableRef, limit);
        return sqlQuery;
    }

    public static void main(String[] args) {
        try {
            File path = new File(".");
            System.out.println(path.getAbsolutePath());
            File jsonKey = new File("workspace-manager-clienttests/rendered/testrunner-service-account.json");
            System.out.println(jsonKey.exists());
            GoogleCredentials serviceAccountCredential = ServiceAccountCredentials.fromStream(new FileInputStream(jsonKey)).createScoped(bigQueryScope);
            System.out.println(serviceAccountCredential.getAccessToken().getTokenValue());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
