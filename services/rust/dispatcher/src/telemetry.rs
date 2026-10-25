use opentelemetry::{
    global,
    metrics::{Counter, Histogram},
    trace::TracerProvider as _,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub struct DispatcherMetrics {
    pub events_total: Counter<u64>,
    pub new_games_total: Counter<u64>,
    pub errors_total: Counter<u64>,
    pub redis_latency_ms: Histogram<f64>,
}

impl DispatcherMetrics {
    pub fn new() -> Self {
        let meter = global::meter("observelol.dispatcher");
        Self {
            events_total: meter
                .u64_counter("ingest.events_total")
                .with_description("Total number of game events ingested")
                .build(),
            new_games_total: meter
                .u64_counter("ingest.new_games_total")
                .with_description("Total number of new games initialized")
                .build(),
            errors_total: meter
                .u64_counter("ingest.errors_total")
                .with_description("Total number of ingestion errors")
                .build(),
            redis_latency_ms: meter
                .f64_histogram("ingest.redis_latency_ms")
                .with_description("Latency of Redis operations in milliseconds")
                .build(),
        }
    }
}

pub fn init_telemetry(otel_endpoint: &str) -> (SdkTracerProvider, SdkMeterProvider) {
    let resource = Resource::builder()
        .with_service_name("observelol-dispatcher")
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .expect("Failed to initialize OTLP gRPC span exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otel_endpoint)
        .build()
        .expect("Failed to initialize OTLP gRPC metric exporter");

    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let tracer = tracer_provider.tracer("observelol-dispatcher");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .with(otel_layer)
        .init();

    (tracer_provider, meter_provider)
}
