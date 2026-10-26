use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
    trace::TracerProvider as _,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub struct WorkerMetrics {
    pub games_processed_total: Counter<u64>,
    pub validation_failures_total: Counter<u64>,
    pub dropped_games_total: Counter<u64>,
    pub errors_total: Counter<u64>,
    pub processing_latency_ms: Histogram<f64>,
    pub redis_latency_ms: Histogram<f64>,
    pub db_latency_ms: Histogram<f64>,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        let meter = global::meter("observelol.worker");
        Self {
            games_processed_total: meter
                .u64_counter("worker.games_processed_total")
                .with_description("Total number of games validated and compiled into replays")
                .build(),
            validation_failures_total: meter
                .u64_counter("worker.validation_failures_total")
                .with_description("Total number of games that failed replay validation")
                .build(),
            dropped_games_total: meter
                .u64_counter("worker.dropped_games_total")
                .with_description(
                    "Total number of games dropped due to empty/expired events or drop mode",
                )
                .build(),
            errors_total: meter
                .u64_counter("worker.errors_total")
                .with_description("Total number of worker operational errors")
                .build(),
            processing_latency_ms: meter
                .f64_histogram("worker.processing_latency_ms")
                .with_description("End-to-end game processing latency in milliseconds")
                .build(),
            redis_latency_ms: meter
                .f64_histogram("worker.redis_latency_ms")
                .with_description("Latency of Redis operations in milliseconds")
                .build(),
            db_latency_ms: meter
                .f64_histogram("worker.db_latency_ms")
                .with_description("Latency of database operations in milliseconds")
                .build(),
        }
    }
}

pub fn init_telemetry(
    otel_endpoint: &str,
    instance_id: &str,
) -> (SdkTracerProvider, SdkMeterProvider) {
    let resource = Resource::builder()
        .with_service_name("observelol-worker")
        .with_attribute(KeyValue::new(
            "service.instance.id",
            instance_id.to_string(),
        ))
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

    let tracer = tracer_provider.tracer("observelol-worker");
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
