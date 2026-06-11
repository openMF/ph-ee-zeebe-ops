package org.mifos.ops.zeebe.zeebe;

public final class ZeebeVariables {

    private ZeebeVariables() {
    }

    public static final String TRANSACTION_ID = "transactionId";
    public static final String BPMN_PROCESS_ID = "BPMN_PROCESS_ID";
    public static final String PROCESS_INSTANCE_KEY = "PROCESS_INSTANCE_KEY";
    public static final String PROCESS_DEFINITION_KEY = "PROCESS_DEFINITION_KEY";

    // REST path-parameter names. Camel 4 compiles {placeholder} into a Java
    // named capturing group, and Java group names cannot contain underscores,
    // so the *_KEY/_ID constants above cannot be used in route templates.
    public static final String BPMN_PROCESS_ID_PARAM = "bpmnProcessId";
    public static final String PROCESS_INSTANCE_KEY_PARAM = "processInstanceKey";
    public static final String PROCESS_DEFINITION_KEY_PARAM = "processDefinitionKey";
}