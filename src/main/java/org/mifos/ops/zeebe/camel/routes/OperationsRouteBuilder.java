package org.mifos.ops.zeebe.camel.routes;

import io.camunda.zeebe.client.ZeebeClient;
import org.apache.camel.LoggingLevel;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.mifos.connector.common.camel.ErrorHandlerRouteBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.mifos.ops.zeebe.zeebe.ZeebeMessages.OPERATOR_MANUAL_RECOVERY;
import static org.mifos.ops.zeebe.zeebe.ZeebeVariables.BPMN_PROCESS_ID;
import static org.mifos.ops.zeebe.zeebe.ZeebeVariables.TRANSACTION_ID;

@Component
public class OperationsRouteBuilder extends ErrorHandlerRouteBuilder {

    @Autowired
    private ZeebeClient zeebeClient;

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

        from("rest:POST:/channel/transaction/{" + TRANSACTION_ID + "}/resolve")
                .id("transaction-resolve")
                .log(LoggingLevel.INFO, "## operator transaction resolve")
                .process(e -> {
                    Map<String, Object> variables = new HashMap<>();
                    JSONObject request = new JSONObject(e.getIn().getBody(String.class));
                    request.keys().forEachRemaining(k -> {
                        variables.put(k, request.get(k));
                    });

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
                    requestedVariables.keys().forEachRemaining(k -> {
                        newVariables.put(k, requestedVariables.get(k));
                    });

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
                    requestedVariables.keys().forEachRemaining(k -> {
                        newVariables.put(k, requestedVariables.get(k));
                    });

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
