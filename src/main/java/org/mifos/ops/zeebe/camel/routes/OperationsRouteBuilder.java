package org.mifos.ops.zeebe.camel.routes;

import io.camunda.zeebe.client.ZeebeClient;
import org.apache.camel.LoggingLevel;
import org.json.JSONArray;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.mifos.connector.common.camel.ErrorHandlerRouteBuilder;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static org.mifos.ops.zeebe.zeebe.ZeebeMessages.OPERATOR_MANUAL_RECOVERY;
import static org.mifos.ops.zeebe.zeebe.ZeebeVariables.BPMN_PROCESS_ID;
import static org.mifos.ops.zeebe.zeebe.ZeebeVariables.TRANSACTION_ID;

@Component
public class OperationsRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private ZeebeClient zeebeClient;

    @Autowired
    private Logger logger;

    private RestHighLevelClient esClient;

    @Override
    public void configure() {

        /**
         * Starts a workflow with the set of variables passed as body parameters
         *
         * method: [POST]
         * request body: {
         *     "var1": "val1",
         *     "var2": "val2"
         * }
         *
         * response body: Null
         *
         * demo url: /channel/workflow/international_remittance_payer_process-ibank-usa
         * Here [international_remittance_payer_process-ibank-usa] is the value of [BPMN_PROCESS_ID] path variable
         *
         */
        from(String.format("rest:POST:/channel/workflow/{%s}", BPMN_PROCESS_ID))
                .id("workflow-start")
                .log(LoggingLevel.INFO, "## Starting new workflow")
                .process(e -> {

                    JSONObject variables = new JSONObject(e.getIn().getBody(String.class));

                    e.getMessage().setBody(e.getIn().getHeader(BPMN_PROCESS_ID, String.class));


                    zeebeClient.newCreateInstanceCommand()
                            .bpmnProcessId(e.getIn().getHeader(BPMN_PROCESS_ID, String.class))
                            .latestVersion()
                            .variables(variables)
                            .send()
                            .join();

                });

        /**
         * Bulk cancellation of active process by processId
         *
         * method: [PUT]
         * request body: {
         *     processId: [123, 456, 789]
         * }
         *
         * response body: {
         *     success: [], # list of processId which was successfully cancelled
         *     failed: [] # list of processId whose cancellation wasn't successful
         *     cancellationSuccessful: int # total number of process which was successfully cancelled
         *     cancellationFailed: int # total number of process whose cancellation wasn't successful
         *
         * }
         */
        from("rest:PUT:/channel/workflow")
                .id("bulk-cancellation")
                .log(LoggingLevel.INFO, "## bulk cancellation by process id")
                .process(exchange -> {

                    JSONObject object = new JSONObject(exchange.getIn().getBody(String.class));
                    JSONArray processIds = object.getJSONArray("processId");

                    JSONArray success = new JSONArray();
                    JSONArray failed = new JSONArray();

                    AtomicInteger successfullyCancelled = new AtomicInteger();
                    AtomicInteger cancellationFailed = new AtomicInteger();


                    processIds.forEach(elm -> {
                        long processId = Long.parseLong(elm.toString());

                        try {
                            zeebeClient.newCancelInstanceCommand(processId).send().join();
                            success.put(processId);
                            successfullyCancelled.getAndIncrement();
                        }catch (Exception e) {
                            failed.put(processId);
                            cancellationFailed.getAndIncrement();
                            logger.error("Cancellation of process id " + processId + " failed\n" + e.getMessage());
                        }

                    });

                    JSONObject response = new JSONObject();
                    response.put("success", success);
                    response.put("failed", failed);
                    response.put("cancellationSuccessful", successfullyCancelled.get());
                    response.put("cancellationFailed", cancellationFailed.get());

                    exchange.getMessage().setBody(response.toString());
                });


        from("rest:get:/es/health")
                .id("es-test")
                .log(LoggingLevel.INFO, "## Testing es connection")
                .process(exchange -> {
                    JSONObject jsonResponse = new JSONObject();
                    try {
                        GetIndexRequest request = new GetIndexRequest("*");
                        esClient.indices().get(request, RequestOptions.DEFAULT);
                        jsonResponse.put("status", "UP");
                    } catch (Exception e) {
                        jsonResponse.put("status", "down");
                        jsonResponse.put("reason", e.getMessage());
                    }

                    exchange.getMessage().setBody(jsonResponse.toString());
                });

        from("rest:POST:/channel/transaction/{" + TRANSACTION_ID + "}/resolve")
                .id("transaction-resolve")
                .log(LoggingLevel.INFO, "## operator transaction resolve")
                .process(e -> {
                    Map<String, Object> variables = new HashMap<>();
                    JSONObject request = new JSONObject(e.getIn().getBody(String.class));
                    request.keys().forEachRemaining(k -> variables.put(k, request.get(k)));

                    zeebeClient.newPublishMessageCommand()
                            .messageName(OPERATOR_MANUAL_RECOVERY)
                            .correlationKey(e.getIn().getHeader(TRANSACTION_ID, String.class))
                            .timeToLive(Duration.ofMillis(30000))
                            .variables(variables)
                            .send()
                            .join();
                })
                .setBody(constant(null));

        from("rest:POST:/channel/job/resolve")
                .id("job-resolve")
                .log(LoggingLevel.INFO, "## operator job resolve")
                .process(e -> {
                    JSONObject request = new JSONObject(e.getIn().getBody(String.class));
                    JSONObject incident = request.getJSONObject("incident");
                    Map<String, Object> newVariables = new HashMap<>();
                    JSONObject requestedVariables = request.getJSONObject("variables");
                    requestedVariables.keys().forEachRemaining(k -> newVariables.put(k, requestedVariables.get(k)));

                    zeebeClient.newSetVariablesCommand(incident.getLong("elementInstanceKey"))
                            .variables(newVariables)
                            .send()
                            .join();

                    zeebeClient.newUpdateRetriesCommand(incident.getLong("jobKey"))
                            .retries(incident.getInt("newRetries"))
                            .send()
                            .join();

                    zeebeClient.newResolveIncidentCommand(incident.getLong("key"))
                            .send()
                            .join();
                })
                .setBody(constant(null));

        from("rest:POST:/channel/workflow/resolve")
                .id("workflow-resolve")
                .log(LoggingLevel.INFO, "## operator workflow resolve")
                .process(e -> {
                    JSONObject request = new JSONObject(e.getIn().getBody(String.class));
                    JSONObject incident = request.getJSONObject("incident");
                    Map<String, Object> newVariables = new HashMap<>();
                    JSONObject requestedVariables = request.getJSONObject("variables");
                    requestedVariables.keys().forEachRemaining(k -> newVariables.put(k, requestedVariables.get(k)));

                    zeebeClient.newSetVariablesCommand(incident.getLong("elementInstanceKey"))
                            .variables(newVariables)
                            .send()
                            .join();

                    zeebeClient.newResolveIncidentCommand(incident.getLong("key"))
                            .send()
                            .join();
                })
                .setBody(constant(null));

        from("rest:POST:/channel/workflow/{workflowInstanceKey}/cancel")
                .id("workflow-cancel")
                .log(LoggingLevel.INFO, "## operator workflow cancel ${header.workflowInstanceKey}")
                .process(e -> zeebeClient.newCancelInstanceCommand(Long.parseLong(e.getIn().getHeader("workflowInstanceKey", String.class)))
                        .send()
                        .join())
                .setBody(constant(null));

    }
}
