package org.pipelineframework;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitOccurrence;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.branching.BranchVariantIdentity;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.cli.LocalFileDevStores;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.command.NativeCommandSelector;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConnectorBinding;
import org.pipelineframework.config.pipeline.PipelineYamlOperationSelection;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderLease;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.ResolvedConnection;
import org.pipelineframework.connector.polyglot.PolyglotConnectorConnection;
import org.pipelineframework.connector.polyglot.PolyglotConnectorProvider;
import org.pipelineframework.connector.polyglot.PolyglotConnectorSpec;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.CanonicalFunctionHostManyToOneStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostOneToManyStep;
import org.pipelineframework.orchestrator.CanonicalFunctionHostStep;
import org.pipelineframework.orchestrator.CoordinationHost;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.JsonDurablePayloadCodec;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.LocalControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutionMode;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TypedDurablePayload;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import org.pipelineframework.processor.util.PipelineContractMetadataGenerator;
import org.pipelineframework.query.InMemoryQueryCaptureStore;
import org.pipelineframework.query.NativeQuerySelector;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.telemetry.NoOpDevTelemetry;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/**
 * Standalone {@code tpf dev} execution engine (ADR-0092).
 *
 * <p>Wires TPF's actual standard execution engine ({@link QueueAsyncCoordinator},
 * {@link PipelineExecutionService}, {@link PipelineRunner}, {@link PipelineStepExecutor},
 * {@link ExecutionFailureHandler}, {@link AwaitCoordinator}, {@link CanonicalFunctionHostStep},
 * {@link CanonicalFunctionHostOneToManyStep}, {@link CanonicalFunctionHostManyToOneStep}, and
 * Tier-2 {@link PolyglotConnectorProvider}) against crash-safe fsynced disk stores
 * ({@link LocalFileDevStores}) with zero CDI container, zero Mockito, and zero Docker.</p>
 */
public final class TpfLocalDevRuntime {

    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    private static final Duration STEP_TIMEOUT = Duration.ofSeconds(10);

    private final PipelineTemplateConfig template;
    private final PipelineContractDescriptor contract;
    private final String catalogFingerprint;
    private final Map<String, String> definitionFingerprints;
    private final CanonicalTypeCatalogue catalogue;
    private final LocalFileDevStores stores;
    private final QueueAsyncCoordinator coordinator;
    private final PipelineExecutionService executionService;
    private final ClassLoader orderClassLoader;
    private final Queue<ExecutionWorkItem> workQueue = new ConcurrentLinkedQueue<>();
    private final Queue<DeadLetterEnvelope> deadLetters = new ConcurrentLinkedQueue<>();
    private final Map<Integer, String> awaitOutputTypes = new LinkedHashMap<>();

    public static void initializeNativeBootstrap() {
        io.quarkus.runtime.ssl.SslContextConfiguration.setSslNativeEnabled(true);
        io.quarkus.runtime.configuration.QuarkusConfigFactory.setConfig(
            new io.smallrye.config.SmallRyeConfigBuilder().build());
    }

    public TpfLocalDevRuntime(
        Path pipelineYaml,
        URI functionHostUri,
        Optional<URI> connectorHostUri,
        String sharedSecret,
        Path dataDir,
        int maxRetries,
        Map<String, String> connectorSecrets,
        boolean allowLoopbackHttp
    ) throws IOException {
        Objects.requireNonNull(pipelineYaml, "pipelineYaml must not be null");
        Objects.requireNonNull(functionHostUri, "functionHostUri must not be null");
        Objects.requireNonNull(connectorHostUri, "connectorHostUri must not be null");
        Objects.requireNonNull(sharedSecret, "sharedSecret must not be null");
        Objects.requireNonNull(dataDir, "dataDir must not be null");
        Objects.requireNonNull(connectorSecrets, "connectorSecrets must not be null");

        this.template = new PipelineTemplateConfigLoader().load(pipelineYaml);
        PipelineYamlConfig yamlConfig = new PipelineYamlConfigLoader().load(pipelineYaml);
        String contractJson = new PipelineContractMetadataGenerator().renderPolyglotContract(template, yamlConfig);
        this.contract = JSON.readValue(contractJson, PipelineContractDescriptor.class);

        JsonNode contractTree = JSON.readTree(contractJson);
        this.catalogFingerprint = contractTree.path("canonicalCatalogFingerprint").asText();
        Map<String, Map<String, Object>> canonicalTypesMap = JSON.convertValue(
            contractTree.path("canonicalTypes"),
            new TypeReference<>() { });
        this.catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(canonicalTypesMap);
        Map<String, String> fps = new LinkedHashMap<>();
        contractTree.path("canonicalTypes").fields().forEachRemaining(entry ->
            fps.put(entry.getKey(), entry.getValue().path("definitionFingerprint").asText()));
        this.definitionFingerprints = Map.copyOf(fps);

        JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
        this.stores = new LocalFileDevStores(dataDir, payloadCodec);

        PipelineOrchestratorConfig config = DevConfigs.orchestratorConfig(contract.pipelineId(), maxRetries);
        PipelineStepConfig stepConfig = DevConfigs.stepConfig();

        ConnectorSupportHolder connectorHolder = buildPolyglotConnectorSupport(
            yamlConfig, connectorHostUri.orElse(functionHostUri), sharedSecret, connectorSecrets,
            allowLoopbackHttp, config);

        List<Object> steps = new ArrayList<>();
        List<Map<String, Object>> edges = new ArrayList<>();
        buildStepsAndEdges(
            yamlConfig, functionHostUri, sharedSecret, allowLoopbackHttp, connectorHolder, steps, edges);

        Files.createDirectories(dataDir);
        Path orderFile = dataDir.resolve("meta-order.json");
        Files.write(orderFile, JSON.writeValueAsBytes(Map.of(
            "order", edges.stream().map(e -> e.get("stepClass")).toList(),
            "boundarySchemaVersion", 1,
            "stepBoundaries", edges,
            "definitionBoundaries", List.of())));
        var orderUrl = orderFile.toUri().toURL();
        this.orderClassLoader = new ClassLoader(TpfLocalDevRuntime.class.getClassLoader()) {
            @Override
            public Enumeration<java.net.URL> getResources(String name) throws IOException {
                return "META-INF/pipeline/order.json".equals(name)
                    ? Collections.enumeration(List.of(orderUrl))
                    : super.getResources(name);
            }
        };

        DurableLocalAwaitCoordinator awaitCoordinator = new DurableLocalAwaitCoordinator(
            stores.interactionStore, stores.unitStore);

        WorkDispatcher dispatcher = new WorkDispatcher() {
            @Override public Uni<Void> enqueueNow(ExecutionWorkItem item) {
                workQueue.add(item);
                return Uni.createFrom().voidItem();
            }
            @Override public Uni<Void> enqueueDelayed(ExecutionWorkItem item, Duration delay) {
                workQueue.add(item);
                return Uni.createFrom().voidItem();
            }
            @Override public String providerName() { return "local-dev-queue"; }
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
        };
        DeadLetterPublisher dlq = new DeadLetterPublisher() {
            @Override public Uni<Void> publish(DeadLetterEnvelope envelope) {
                deadLetters.add(envelope);
                return Uni.createFrom().voidItem();
            }
            @Override public Optional<String> startupValidationError() { return Optional.empty(); }
        };

        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
        ExecutionFailureHandler failureHandler = new ExecutionFailureHandler();
        failureHandler.orchestratorConfig = config;
        failureHandler.segmentBoundaryLedger = ledger;

        PipelineReleaseIdentityResolver identityResolver = new PipelineReleaseIdentityResolver() {
            @Override public Optional<PipelineContractDescriptor> compiledContract() {
                return Optional.of(contract);
            }
        };

        this.coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = config;
        coordinator.executionStateStore = stores.executionStore;
        coordinator.workDispatcher = dispatcher;
        coordinator.deadLetterPublisher = dlq;
        coordinator.awaitCoordinator = awaitCoordinator;
        coordinator.checkpointPublicationService = new CheckpointPublicationService() {
            @Override public boolean enabled() { return false; }
            @Override public Uni<Void> publishIfConfigured(ExecutionRecord<Object, Object> r, Object payload) {
                return Uni.createFrom().voidItem();
            }
        };
        coordinator.objectPublishCompletionService = new ObjectPublishCompletionService() {
            @Override public boolean enabled() { return false; }
        };
        coordinator.executionInputPolicy = new ExecutionInputPolicy();
        coordinator.executionInputPolicy.orchestratorConfig = config;
        coordinator.executionResultShapeResolver = new ExecutionResultShapeResolver() {
            @Override public ExecutionResultShape resolve() { return ExecutionResultShape.SINGLE; }
        };
        coordinator.executionFailureHandler = failureHandler;
        coordinator.segmentBoundaryLedger = ledger;
        coordinator.controlPlaneAdmissionPolicy = new LocalControlPlaneAdmissionPolicy();
        coordinator.releaseIdentityResolver = identityResolver;
        coordinator.transitionPayloadCodec = payloadCodec;
        coordinator.transitionWorkerExecutor = new TransitionWorkerExecutor(config, new PipelineInvocationRuntime());

        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.defaults().retryLimit(0);
        NoOpDevTelemetry telemetry = new NoOpDevTelemetry();

        PipelineRunner runner = new PipelineRunner();
        runner.configFactory = new ConfigFactory();
        runner.pipelineConfig = pipelineConfig;
        runner.runTelemetry = telemetry;
        runner.stepTelemetry = telemetry;
        runner.stepOrderer = new PipelineStepOrderer() {
            @Override List<Object> orderSteps(List<Object> inputSteps) { return inputSteps; }
        };
        runner.parallelismPolicyResolver = new PipelineParallelismPolicyResolver() {
            @Override ParallelismPolicy resolveParallelismPolicy(PipelineConfig c) { return ParallelismPolicy.SEQUENTIAL; }
            @Override int resolveMaxConcurrency(PipelineConfig c) { return 1; }
        };
        runner.cacheSupportFactory = new PipelineCacheSupportFactory(null, null, "none") {
            @Override PipelineRunner.CacheReadSupport buildCacheReadSupport() { return null; }
        };
        runner.stepExecutor = new PipelineStepExecutor();

        LocalPipelineControlPlane localControlPlane = new LocalPipelineControlPlane() {
            @Override public void initializeQueueMode() { }
        };
        localControlPlane.queueAsyncCoordinator = coordinator;

        this.executionService = new PipelineExecutionService();
        executionService.orchestratorConfig = config;
        executionService.pipelineConfig = pipelineConfig;
        executionService.pipelineRunner = runner;
        executionService.healthCheckService = new HealthCheckService() {
            @Override public boolean checkHealthOfDependentServices(List<Object> s) { return true; }
        };
        executionService.controlPlane = localControlPlane;
        executionService.configFactory = new ConfigFactory();
        executionService.pipelineStepResolver = new PipelineStepResolver() {
            @Override List<Object> loadPipelineSteps() { return List.copyOf(steps); }
        };
        executionService.pipelineStepConfig = stepConfig;
        executionService.stepOrderer = runner.stepOrderer;
        executionService.awaitCoordinator = awaitCoordinator;
        executionService.executionHooks = new ExecutionHooks();
        executionService.executionInputPolicy = coordinator.executionInputPolicy;
        executionService.transitionPayloadCodec = payloadCodec;
        executionService.releaseIdentityResolver = identityResolver;
        localControlPlane.pipelineExecutionService = executionService;
        executionService.runStartupHealthChecks();
    }

    private ConnectorSupportHolder buildPolyglotConnectorSupport(
        PipelineYamlConfig yamlConfig,
        URI connectorHostUri,
        String sharedSecret,
        Map<String, String> connectorSecrets,
        boolean allowLoopbackHttp,
        PipelineOrchestratorConfig orchestratorConfig
    ) {
        Map<String, PipelineYamlStep> yamlByToken = indexYamlSteps(yamlConfig);
        List<PolyglotConnectorSpec.QueryOperationSpec> queries = new ArrayList<>();
        List<PolyglotConnectorSpec.CommandOperationSpec> commands = new ArrayList<>();
        Map<String, ConnectorBindingName> polyglotBindings = new LinkedHashMap<>();

        for (PipelineTemplateStep step : template.steps()) {
            PipelineYamlStep ys = yamlByToken.get(normalizeToken(step.name()));
            if (ys == null || ys.operationSelection().isEmpty()) {
                continue;
            }
            PipelineYamlOperationSelection sel = ys.operationSelection().orElseThrow();
            PipelineYamlConnectorBinding binding = yamlConfig.connectors().get(sel.using());
            if (binding == null || !"polyglot.connector".equalsIgnoreCase(binding.provider())) {
                continue;
            }
            polyglotBindings.putIfAbsent(sel.using(), ConnectorBindingName.of(sel.using()));
            String kind = ys.kind().trim().toLowerCase(Locale.ROOT);
            int major = sel.operationVersion();
            if ("query".equals(kind)) {
                queries.add(new PolyglotConnectorSpec.QueryOperationSpec(
                    sel.operation(), major, step.inputTypeName(), step.outputTypeName()));
            } else if ("command".equals(kind)) {
                Map<String, Object> cfg = binding.config() == null ? Map.of() : binding.config();
                boolean reconcile = Boolean.parseBoolean(String.valueOf(cfg.getOrDefault("reconciliationSupported", false)));
                Optional<String> refKind = Optional.ofNullable(cfg.get("receiptReferenceKind"))
                    .map(Object::toString).filter(v -> !v.isBlank());
                commands.add(new PolyglotConnectorSpec.CommandOperationSpec(
                    sel.operation(), major, step.inputTypeName(), step.outputTypeName(), reconcile, refKind));
            }
        }
        if (queries.isEmpty() && commands.isEmpty()) {
            return new ConnectorSupportHolder(Optional.empty(), Optional.empty());
        }

        PolyglotConnectorSpec spec = new PolyglotConnectorSpec(catalogue, queries, commands);
        PolyglotConnectorProvider provider = new PolyglotConnectorProvider(spec);
        ConnectionResolver resolver = new ConnectionResolver() {
            @Override
            public <T extends ResolvedConnection> CompletionStage<T> resolve(ConnectionResolutionRequest<T> request) {
                if (request.connectionType().equals(PolyglotConnectorConnection.class)) {
                    PolyglotConnectorConnection conn = new PolyglotConnectorConnection(
                        connectorHostUri, sharedSecret, connectorSecrets, allowLoopbackHttp);
                    return CompletableFuture.completedStage(request.connectionType().cast(conn));
                }
                return CompletableFuture.failedStage(
                    new IllegalArgumentException("Unknown connection type: " + request.connectionType().getName()));
            }
        };
        ConnectorRuntimeContext runtimeContext = ConnectorRuntimeContext.of(
            "tpf-local-dev", Executors.newCachedThreadPool(), Clock.systemUTC(), Optional.of(resolver));
        List<ConnectorBindingDefinition> bindingDefs = new ArrayList<>();
        for (ConnectorBindingName bindingName : polyglotBindings.values()) {
            bindingDefs.add(new ConnectorBindingDefinition(
                bindingName,
                PolyglotConnectorProvider.PROVIDER_ID,
                1,
                new ConnectorConfigurationDocument(Map.of("connection", "local-polyglot-host"))));
        }
        ConnectorBindingRegistry bindingRegistry = ConnectorBindingRegistry.fromProviders(
            bindingDefs,
            List.of(provider),
            p -> ConnectorProviderLease.of(p, () -> { }));
        InMemoryCommandEffectStore effectStore = new InMemoryCommandEffectStore();
        InMemoryQueryCaptureStore captureStore = new InMemoryQueryCaptureStore();
        CommandStepSupport commandSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of()), bindingRegistry, runtimeContext, List.of(effectStore), orchestratorConfig);
        QueryStepSupport querySupport = new QueryStepSupport(
            List.of(), List.of(captureStore), bindingRegistry, runtimeContext);
        return new ConnectorSupportHolder(Optional.of(querySupport), Optional.of(commandSupport));
    }

    private void buildStepsAndEdges(
        PipelineYamlConfig yamlConfig,
        URI functionHostUri,
        String sharedSecret,
        boolean allowLoopbackHttp,
        ConnectorSupportHolder connectorHolder,
        List<Object> steps,
        List<Map<String, Object>> edges
    ) {
        Map<String, PipelineYamlStep> yamlByToken = indexYamlSteps(yamlConfig);
        for (int i = 0; i < template.steps().size(); i++) {
            PipelineTemplateStep step = template.steps().get(i);
            PipelineYamlStep ys = yamlByToken.get(normalizeToken(step.name()));
            String kind = ys == null ? "internal" : ys.kind().trim().toLowerCase(Locale.ROOT);
            String stepId = toKebabCase(step.name());
            String inputType = step.inputTypeName();
            String outputType = step.outputTypeName();
            String inputFp = definitionFingerprints.get(inputType);
            String outputFp = definitionFingerprints.get(outputType);
            boolean isAwait = "await".equals(kind)
                || "awaitable".equals(kind)
                || stepId.startsWith("await-");

            Object instance;
            if (isAwait) {
                awaitOutputTypes.put(i, outputType);
                instance = new DurableCanonicalAwaitStep(
                    stores.unitStore, stores.interactionStore, i, stepId, outputType);
            } else if (("query".equals(kind) || "command".equals(kind))
                && ys != null && ys.operationSelection().isPresent()) {
                PipelineYamlOperationSelection sel = ys.operationSelection().orElseThrow();
                PipelineYamlConnectorBinding binding = yamlConfig.connectors().get(sel.using());
                if (binding != null && "polyglot.connector".equalsIgnoreCase(binding.provider())) {
                    int major = sel.operationVersion();
                    if ("query".equals(kind)) {
                        QueryStepDescriptor desc = QueryStepDescriptor.nativeQuery(
                            stepId,
                            JsonNode.class.getName(),
                            JsonNode.class.getName(),
                            "ONE_TO_ONE",
                            new NativeQuerySelector(
                                ConnectorBindingName.of(sel.using()),
                                new ConnectorOperationIdentity(
                                    PolyglotConnectorProvider.PROVIDER_ID, sel.operation(),
                                    ConnectorOperationKind.QUERY, major),
                                1),
                            Map.of());
                        instance = new LocalPolyglotQueryStep(
                            connectorHolder.querySupport().orElseThrow(),
                            desc, outputType, outputFp, catalogFingerprint);
                    } else {
                        Map<String, Object> cfg = binding.config() == null ? Map.of() : binding.config();
                        boolean reconcile = Boolean.parseBoolean(String.valueOf(cfg.getOrDefault("reconciliationSupported", false)));
                        CommandDescriptor desc = CommandDescriptor.nativeCommand(
                            stepId,
                            new NativeCommandSelector(
                                Optional.of(ConnectorBindingName.of(sel.using())),
                                new ConnectorOperationIdentity(
                                    PolyglotConnectorProvider.PROVIDER_ID, sel.operation(),
                                    ConnectorOperationKind.COMMAND, major),
                                1,
                                new CommandPolicy(true, true, reconcile, Optional.empty(),
                                    Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), false)),
                            JsonNode.class.getName(),
                            JsonNode.class.getName(),
                            stepId + "-id",
                            CommandDuplicatePolicy.RETURN_RECORDED,
                            Map.of());
                        instance = new LocalPolyglotCommandStep(
                            connectorHolder.commandSupport().orElseThrow(),
                            desc, stepId, outputType, outputFp, catalogFingerprint);
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Unsupported local dev connector provider '"
                            + (binding == null ? sel.using() : binding.provider()) + "' on step '" + step.name() + "'");
                }
            } else {
                String card = normalizeCardinality(step.cardinality());
                Optional<String> cmdId = Optional.of("cmd-" + stepId);
                instance = switch (card) {
                    case "ONE_TO_ONE" -> new CanonicalFunctionHostStep(
                        new CanonicalFunctionHostStep.Spec(
                            stepId, i, inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, functionHostUri, sharedSecret,
                            STEP_TIMEOUT, cmdId).withDispatchIntentStore(stores.dispatchIntentStore),
                        allowLoopbackHttp);
                    case "ONE_TO_MANY" -> new CanonicalFunctionHostOneToManyStep(
                        new CanonicalFunctionHostOneToManyStep.Spec(
                            stepId, i, inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, functionHostUri, sharedSecret,
                            STEP_TIMEOUT, STEP_TIMEOUT, cmdId, stores.dispatchIntentStore,
                            4 * 1024 * 1024),
                        allowLoopbackHttp);
                    case "MANY_TO_ONE" -> new CanonicalFunctionHostManyToOneStep(
                        new CanonicalFunctionHostManyToOneStep.Spec(
                            stepId, i, inputType, inputFp, outputType, outputFp,
                            catalogFingerprint, catalogue, functionHostUri, sharedSecret,
                            STEP_TIMEOUT, cmdId, stores.dispatchIntentStore,
                            4 * 1024 * 1024),
                        allowLoopbackHttp);
                    default -> throw new IllegalArgumentException(
                        "Cardinality '" + card + "' is not supported by tpf dev (ADR-0092)");
                };
            }
            steps.add(instance);
            edges.add(Map.of(
                "stepClass", instance.getClass().getName(),
                "inputTypeId", inputType,
                "outputTypeId", outputType));
        }
    }

    public void drainQueue() {
        int safety = 0;
        while (!workQueue.isEmpty() && safety++ < 50) {
            ExecutionWorkItem item = workQueue.poll();
            if (item == null) {
                break;
            }
            PipelineTransitionWorker tcclWorker = new PipelineTransitionWorker() {
                @Override
                public Uni<TransitionResultEnvelope> executeTransition(TransitionCommandEnvelope command) {
                    ClassLoader previous = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(orderClassLoader);
                    try {
                        return executionService.executeTransition(command);
                    } finally {
                        Thread.currentThread().setContextClassLoader(previous);
                    }
                }
                @Override public String providerName() { return executionService.providerName(); }
            };
            ClassLoader previous = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(orderClassLoader);
            try {
                coordinator.processExecutionWorkItem(item, tcclWorker).await().atMost(TIMEOUT);
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }
    }

    public HttpServer startHttp(int port, long startupStartNanos, PrintWriter out) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.createContext("/health", ex -> respond(ex, 200, Map.of(
            "status", "UP",
            "pid", ProcessHandle.current().pid(),
            "pipelineId", contract.pipelineId(),
            "contractVersion", contract.contractVersion(),
            "catalogFingerprint", catalogFingerprint)));
        server.createContext("/runs", this::handleRuns);
        server.createContext("/awaits/complete", this::handleAwaitComplete);
        server.createContext("/sweep", this::handleSweep);
        server.start();
        long startupMs = (System.nanoTime() - startupStartNanos) / 1_000_000L;
        out.println("READY " + server.getAddress().getPort() + " " + startupMs);
        out.flush();
        return server;
    }

    private void handleRuns(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            if ("POST".equals(ex.getRequestMethod()) && "/runs".equals(path)) {
                JsonNode body = JSON.readTree(ex.getRequestBody().readAllBytes());
                String tenant = body.path("tenantId").asText("default");
                String key = body.path("executionKey").asText();
                String inputType = template.steps().getFirst().inputTypeName();
                byte[] canonicalBytes = JSON.writeValueAsBytes(body.path("payload"));
                TypedDurablePayload input = new TypedDurablePayload(
                    inputType,
                    definitionFingerprints.get(inputType),
                    catalogFingerprint,
                    JsonDurablePayloadCodec.ENCODING,
                    JsonDurablePayloadCodec.ENCODING_VERSION,
                    canonicalBytes);
                var accepted = coordinator.executePipelineAsync(input, tenant, key, false).await().atMost(TIMEOUT);
                drainQueue();
                respond(ex, 200, executionView(tenant, accepted.executionId()));
                return;
            }
            if ("GET".equals(ex.getRequestMethod()) && path.startsWith("/runs/")) {
                String[] parts = path.substring("/runs/".length()).split("/");
                if (parts.length == 2) {
                    respond(ex, 200, executionView(parts[0], parts[1]));
                    return;
                }
            }
            respond(ex, 404, Map.of("error", "not_found"));
        } catch (Exception e) {
            respond(ex, 500, Map.of("error", e.getClass().getSimpleName(), "message", String.valueOf(e.getMessage())));
        }
    }

    private void handleAwaitComplete(HttpExchange ex) throws IOException {
        try {
            JsonNode body = JSON.readTree(ex.getRequestBody().readAllBytes());
            String tenant = body.path("tenantId").asText("default");
            String interactionId = body.path("interactionId").asText();
            String correlationId = body.path("correlationId").asText();
            String completionId = body.path("completionId").asText("comp-" + System.nanoTime());
            AwaitInteractionRecord interaction = stores.interactionStore.get(tenant, interactionId)
                .await().atMost(TIMEOUT)
                .orElseThrow(() -> new IllegalArgumentException("Unknown Await interactionId: " + interactionId));
            String outputType = awaitOutputTypes.getOrDefault(interaction.stepIndex(), interaction.outputType());
            TypedDurablePayload responsePayload = new TypedDurablePayload(
                outputType,
                definitionFingerprints.get(outputType),
                catalogFingerprint,
                JsonDurablePayloadCodec.ENCODING,
                JsonDurablePayloadCodec.ENCODING_VERSION,
                JSON.writeValueAsBytes(body.path("payload")));
            var result = coordinator.completeAwait(new AwaitCompletionCommand(
                tenant, interactionId, correlationId, completionId, responsePayload,
                "local-dev-completer", System.currentTimeMillis())).await().atMost(TIMEOUT);
            drainQueue();
            Map<String, Object> resp = new LinkedHashMap<>(executionView(tenant, result.record().executionId()));
            resp.put("duplicate", result.duplicate());
            respond(ex, 200, resp);
        } catch (Exception e) {
            respond(ex, 500, Map.of("error", e.getClass().getSimpleName(), "message", String.valueOf(e.getMessage())));
        }
    }

    private void handleSweep(HttpExchange ex) throws IOException {
        try {
            byte[] raw = ex.getRequestBody().readAllBytes();
            JsonNode body = raw.length == 0 ? JSON.createObjectNode() : JSON.readTree(raw);
            long now = body.path("nowEpochMs").asLong(System.currentTimeMillis());
            var result = coordinator.sweepOnce(now).await().atMost(TIMEOUT);
            drainQueue();
            respond(ex, 200, Map.of(
                "dueDispatched", result.dispatchedExecutionCount(),
                "timedOut", result.timedOutAwaitCount()));
        } catch (Exception e) {
            respond(ex, 500, Map.of("error", e.getClass().getSimpleName(), "message", String.valueOf(e.getMessage())));
        }
    }

    private Map<String, Object> executionView(String tenantId, String executionId) throws IOException {
        ExecutionRecord<Object, Object> rec = stores.executionStore.getExecution(tenantId, executionId)
            .await().atMost(TIMEOUT).orElseThrow();
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("executionId", rec.executionId());
        view.put("tenantId", rec.tenantId());
        view.put("status", rec.status().name());
        view.put("version", rec.version());
        view.put("currentStepIndex", rec.currentStepIndex());
        view.put("attempt", rec.attempt());
        view.put("failedStepIndex", rec.failedStepIndex());
        rec.failedCommandId().ifPresent(v -> view.put("failedCommandId", v));
        if (rec.errorCode() != null) view.put("errorCode", rec.errorCode());
        if (rec.errorMessage() != null) view.put("errorMessage", rec.errorMessage());
        view.put("deadLetterCount", deadLetters.size());
        if (rec.awaitUnitId() != null) {
            List<AwaitInteractionRecord> ints = stores.interactionStore.findByUnit(tenantId, rec.awaitUnitId())
                .await().atMost(TIMEOUT);
            if (!ints.isEmpty()) {
                view.put("interactionId", ints.getFirst().interactionId());
                view.put("correlationId", ints.getFirst().correlationId());
            }
        }
        if (rec.status() == ExecutionStatus.SUCCEEDED) {
            Object raw = coordinator.getExecutionResultPayload(tenantId, executionId).await().atMost(TIMEOUT);
            Object decoded = raw instanceof SerializedTransitionPayload s
                ? coordinator.transitionPayloadCodec.decode(s)
                : raw;
            if (decoded instanceof TypedDurablePayload typed) {
                view.put("resultTypeId", typed.canonicalTypeId());
                view.put("result", JSON.readTree(typed.payload()));
            }
        }
        return view;
    }

    private static void respond(HttpExchange ex, int status, Object body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static Map<String, PipelineYamlStep> indexYamlSteps(PipelineYamlConfig yamlConfig) {
        Map<String, PipelineYamlStep> byToken = new LinkedHashMap<>();
        for (PipelineYamlStep s : yamlConfig.steps()) {
            if (s != null && s.name() != null) {
                byToken.put(normalizeToken(s.name()), s);
            }
        }
        return byToken;
    }

    private static String normalizeToken(String raw) {
        return raw.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    }

    private static String toKebabCase(String raw) {
        String spaced = raw.replaceAll("([a-z0-9])([A-Z])", "$1-$2")
            .replaceAll("[^a-zA-Z0-9]+", "-")
            .toLowerCase(Locale.ROOT);
        String trimmed = spaced.replaceAll("^-+|-+$", "");
        return trimmed.isEmpty() ? "step" : trimmed;
    }

    private static String normalizeCardinality(String raw) {
        if (raw == null || raw.isBlank()) {
            return "ONE_TO_ONE";
        }
        return switch (raw.trim().toUpperCase(Locale.ROOT)) {
            case "ONE_TO_ONE", "UNARY" -> "ONE_TO_ONE";
            case "ONE_TO_MANY", "EXPANSION" -> "ONE_TO_MANY";
            case "MANY_TO_ONE", "REDUCTION" -> "MANY_TO_ONE";
            default -> raw.trim().toUpperCase(Locale.ROOT);
        };
    }

    private record ConnectorSupportHolder(
        Optional<QueryStepSupport> querySupport,
        Optional<CommandStepSupport> commandSupport
    ) {
    }

    private static final class LocalPolyglotQueryStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final QueryStepSupport querySupport;
        private final QueryStepDescriptor descriptor;
        private final String outputType;
        private final String outputFingerprint;
        private final String catalogFingerprint;

        private LocalPolyglotQueryStep(
            QueryStepSupport querySupport,
            QueryStepDescriptor descriptor,
            String outputType,
            String outputFingerprint,
            String catalogFingerprint
        ) {
            this.querySupport = querySupport;
            this.descriptor = descriptor;
            this.outputType = outputType;
            this.outputFingerprint = outputFingerprint;
            this.catalogFingerprint = catalogFingerprint;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            try {
                JsonNode node = JSON.readTree(input.payload());
                return querySupport.queryOneToOne(descriptor, node, JsonNode.class)
                    .map(outNode -> {
                        try {
                            return new TypedDurablePayload(
                                outputType, outputFingerprint, catalogFingerprint,
                                JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
                                JSON.writeValueAsBytes(outNode));
                        } catch (IOException e) {
                            throw new IllegalStateException("Failed serializing query output", e);
                        }
                    });
            } catch (IOException e) {
                return Uni.createFrom().failure(e);
            }
        }
    }

    private static final class LocalPolyglotCommandStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final CommandStepSupport commandSupport;
        private final CommandDescriptor descriptor;
        private final String stepId;
        private final String outputType;
        private final String outputFingerprint;
        private final String catalogFingerprint;

        private LocalPolyglotCommandStep(
            CommandStepSupport commandSupport,
            CommandDescriptor descriptor,
            String stepId,
            String outputType,
            String outputFingerprint,
            String catalogFingerprint
        ) {
            this.commandSupport = commandSupport;
            this.descriptor = descriptor;
            this.stepId = stepId;
            this.outputType = outputType;
            this.outputFingerprint = outputFingerprint;
            this.catalogFingerprint = catalogFingerprint;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            try {
                JsonNode node = JSON.readTree(input.payload());
                String commandId = stepId + "-" + java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(input.payload())).substring(0, 16);
                return commandSupport.<JsonNode, JsonNode>execute(descriptor, (d, i) -> commandId, node)
                    .map(outNode -> {
                        try {
                            return new TypedDurablePayload(
                                outputType, outputFingerprint, catalogFingerprint,
                                JsonDurablePayloadCodec.ENCODING, JsonDurablePayloadCodec.ENCODING_VERSION,
                                JSON.writeValueAsBytes(outNode));
                        } catch (IOException e) {
                            throw new IllegalStateException("Failed serializing command output", e);
                        }
                    });
            } catch (Exception e) {
                return Uni.createFrom().failure(e);
            }
        }
    }

    private static final class DurableCanonicalAwaitStep extends ConfigurableStep
        implements StepOneToOne<TypedDurablePayload, TypedDurablePayload> {
        private final AwaitUnitStore unitStore;
        private final AwaitInteractionStore interactionStore;
        private final int stepIndex;
        private final String stepId;
        private final String outputType;

        private DurableCanonicalAwaitStep(
            AwaitUnitStore unitStore,
            AwaitInteractionStore interactionStore,
            int stepIndex,
            String stepId,
            String outputType
        ) {
            this.unitStore = unitStore;
            this.interactionStore = interactionStore;
            this.stepIndex = stepIndex;
            this.stepId = stepId;
            this.outputType = outputType;
        }

        @Override
        public Uni<TypedDurablePayload> applyOneToOne(TypedDurablePayload input) {
            var ctx = AwaitExecutionContextHolder.get();
            if (ctx == null) {
                return Uni.createFrom().failure(new IllegalStateException("Missing AwaitExecutionContext"));
            }
            long now = System.currentTimeMillis();
            long deadlineMs = now + Duration.ofMinutes(10).toMillis();
            long ttlEpochS = (now / 1000L) + 3600L;
            String unitId = new AwaitUnitOccurrence(
                ctx.tenantId(), ctx.executionId(), ctx.identityScope(), stepId, stepIndex,
                ctx.awaitOccurrenceCause().orElseThrow()).unitId();
            String correlationId = "corr-" + ctx.executionId() + "-step" + stepIndex;
            return unitStore.createOrGet(new AwaitUnitCreateCommand(
                    ctx.tenantId(), unitId, ctx.executionId(), stepId, stepIndex, "ONE_TO_ONE",
                    now, ttlEpochS))
                .chain(() -> interactionStore.createOrGet(new AwaitCreateCommand(
                    ctx.tenantId(), ctx.executionId(), stepId, stepIndex,
                    outputType, outputType,
                    "causation-" + ctx.executionId(), "idem-" + ctx.executionId(), correlationId,
                    input, "reviewer", "approvals", "callback", unitId, null,
                    Map.of("transport", "callback"), now, deadlineMs, ttlEpochS, Optional.empty())))
                .chain(created -> unitStore.attachPrimaryInteraction(
                        ctx.tenantId(), unitId, created.record().interactionId(), now + 1L)
                    .chain(() -> unitStore.markDispatchComplete(ctx.tenantId(), unitId, 1, now + 2L))
                    .chain(() -> Uni.createFrom().failure(new AwaitSuspendedException(
                        ctx.tenantId(), ctx.executionId(), unitId, stepIndex))));
        }
    }

    private static final class DurableLocalAwaitCoordinator extends AwaitCoordinator {
        private final AwaitInteractionStore interactions;
        private final AwaitUnitStore units;

        private DurableLocalAwaitCoordinator(AwaitInteractionStore interactions, AwaitUnitStore units) {
            this.interactions = interactions;
            this.units = units;
        }

        @Override public Uni<Void> importSuspension(TransitionAwaitSuspension suspension) { return Uni.createFrom().voidItem(); }
        @Override public Uni<TransitionAwaitSuspension> suspensionSnapshot(AwaitSuspendedException s) {
            return Uni.createFrom().item(new TransitionAwaitSuspension(s.tenantId(), s.executionId(), s.unitId(), s.stepIndex()));
        }
        @Override public Uni<org.pipelineframework.awaitable.AwaitCompletionResult> complete(AwaitCompletionCommand cmd) {
            return interactions.complete(cmd);
        }
        @Override public Uni<AwaitUnitRecord> recordCompletion(AwaitInteractionRecord record, long nowEpochMs) {
            return units.markCompleted(record.tenantId(), record.unitId(), nowEpochMs)
                .map(u -> u.orElseThrow(() -> new IllegalStateException("Await unit OCC conflict")));
        }
        @Override public Uni<AwaitUnitRecord> getUnit(String tenantId, String unitId) {
            return units.get(tenantId, unitId)
                .map(u -> u.orElseThrow(() -> new IllegalStateException("Await unit not found")));
        }
        @Override public Uni<Object> loadResumePayload(String tenantId, String unitId) {
            return resumeInteraction(tenantId, unitId).map(AwaitInteractionRecord::responsePayload);
        }
        @Override public Uni<Object> loadResumeInput(String tenantId, String unitId) {
            return resumeInteraction(tenantId, unitId).map(AwaitInteractionRecord::responsePayload);
        }
        @Override public Uni<List<AwaitInteractionRecord>> findDueTimeoutConsequences(long now, int limit) {
            return interactions.findDueTimeoutConsequences(now, limit);
        }
        @Override public Uni<List<AwaitInteractionRecord>> findTimedOut(long now, int limit) {
            return interactions.findTimedOut(now, limit);
        }
        private Uni<AwaitInteractionRecord> resumeInteraction(String tenantId, String unitId) {
            return getUnit(tenantId, unitId)
                .chain(u -> interactions.get(tenantId, u.primaryInteractionId()))
                .map(r -> r.orElseThrow(() -> new IllegalStateException("Await interaction not found")));
        }
    }

    private static final class DevConfigs {
        private DevConfigs() { }

        private static PipelineOrchestratorConfig orchestratorConfig(String pipelineId, int maxRetries) {
            return new PipelineOrchestratorConfig() {
                @Override public OrchestratorMode mode() { return OrchestratorMode.QUEUE_ASYNC; }
                @Override public CoordinationHost coordinationHost() { return CoordinationHost.NATIVE; }
                @Override public boolean processLoopsDisabled() { return false; }
                @Override public String defaultTenant() { return "default"; }
                @Override public String pipelineId() { return pipelineId; }
                @Override public String releaseVersion() { return "dev-local"; }
                @Override public int executionTtlDays() { return 1; }
                @Override public long leaseMs() { return Duration.ofMinutes(2).toMillis(); }
                @Override public int maxRetries() { return maxRetries; }
                @Override public Optional<Duration> maxCircuitDeferral() { return Optional.empty(); }
                @Override public Duration retryDelay() { return Duration.ofMillis(10); }
                @Override public double retryMultiplier() { return 1.0d; }
                @Override public Duration sweepInterval() { return Duration.ofSeconds(30); }
                @Override public int sweepLimit() { return 50; }
                @Override public OrchestratorIdempotencyPolicy idempotencyPolicy() { return OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED; }
                @Override public String stateProvider() { return ""; }
                @Override public String dispatcherProvider() { return ""; }
                @Override public String dlqProvider() { return ""; }
                @Override public Optional<String> queueUrl() { return Optional.empty(); }
                @Override public Optional<String> dlqUrl() { return Optional.empty(); }
                @Override public Optional<String> resumeTokenSecret() { return Optional.empty(); }
                @Override public DynamoConfig dynamo() { return null; }
                @Override public SqsConfig sqs() { return null; }
                @Override public TenancyConfig tenancy() { return null; }
                @Override public QuotaConfig quotas() { return null; }
                @Override public ControlPlaneConfig controlPlane() { return null; }
                @Override public AdminConfig admin() { return null; }
                @Override public ReleasesConfig releases() { return null; }
                @Override public WorkerConfig worker() { return WORKER_CONFIG; }
                @Override public boolean strictStartup() { return false; }
            };
        }

        private static final PipelineOrchestratorConfig.WorkerConfig WORKER_CONFIG =
            new PipelineOrchestratorConfig.WorkerConfig() {
                @Override public PipelineOrchestratorConfig.WorkerAwaitAuthorityConfig awaitAuthority() { return null; }
                @Override public String targetingMode() { return "local"; }
                @Override public Map<String, PipelineOrchestratorConfig.RegisteredRestTargetConfig> targets() { return Map.of(); }
                @Override public boolean allowLoopbackHttp() { return true; }
                @Override public TransitionWorkerExecutionMode executionMode() { return TransitionWorkerExecutionMode.SAME_THREAD; }
                @Override public int maxInFlight() { return 16; }
                @Override public Duration saturatedDelay() { return Duration.ofMillis(50); }
                @Override public List<String> allowedPayloadPrefixes() { return List.of(); }
                @Override public Optional<String> artifactId() { return Optional.empty(); }
                @Override public Optional<String> artifactDigest() { return Optional.empty(); }
                @Override public PipelineOrchestratorConfig.RestWorkerConfig rest() { return null; }
                @Override public PipelineOrchestratorConfig.GrpcWorkerConfig grpc() { return null; }
                @Override public PipelineOrchestratorConfig.SqsWorkerConfig sqs() { return null; }
                @Override public PipelineOrchestratorConfig.WorkerLifecycleConfig lifecycle() { return null; }
            };

        private static PipelineStepConfig stepConfig() {
            return new PipelineStepConfig() {
                @Override public StepConfig defaults() { return null; }
                @Override public ParallelismPolicy parallelism() { return ParallelismPolicy.SEQUENTIAL; }
                @Override public Integer maxConcurrency() { return 1; }
                @Override public Integer maxRecursiveDepth() { return 64; }
                @Override public AwaitAdmissionConfig awaitAdmission() { return null; }
                @Override public CallbackConfig callback() { return () -> true; }
                @Override public HealthConfig health() {
                    return new HealthConfig() {
                        @Override public Boolean enabled() { return false; }
                        @Override public Duration startupTimeout() { return Duration.ofSeconds(5); }
                        @Override public String restPath() { return "/q/health"; }
                    };
                }
                @Override public CacheConfig cache() { return null; }
                @Override public TelemetryConfig telemetry() { return null; }
                @Override public KillSwitchConfig killSwitch() { return null; }
                @Override public Map<String, StepConfig> step() { return Map.of(); }
                @Override public Map<String, ModuleConfig> module() { return Map.of(); }
                @Override public ClientConfig client() { return null; }
            };
        }
    }
}
