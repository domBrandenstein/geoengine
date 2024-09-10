use flexi_logger::writers::{FileLogWriter, FileLogWriterHandle};
use flexi_logger::{Age, Cleanup, Criterion, FileSpec, Naming, WriteMode};
use geoengine_operators::processing::initialize_expression_dependencies;
use geoengine_services::error::Result;
use geoengine_services::util::config;
use geoengine_services::util::config::get_config_element;
use tracing::Subscriber;
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::format::{DefaultFields, Writer};
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::layer::Filter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;

#[tokio::main]
async fn main() {
    initialize_expression_dependencies()
        .await
        .expect("successful compilation process is necessary for expression operators to work");

    start_server().await.expect("the server has to start");
}

#[cfg(not(feature = "pro"))]
/// Starts the server.
///
///  # Panics
///  - if the logging configuration is not valid
///
pub async fn start_server() -> Result<()> {
    reroute_gdal_logging();
    let logging_config: config::Logging = get_config_element()?;
    configure_error_report_formatting(&logging_config);

    // get a new tracing subscriber registry to add all log and tracing layers to
    let registry = tracing_subscriber::Registry::default();

    // create a filter for the log message level in console output
    let console_filter =
        EnvFilter::try_new(&logging_config.log_spec).expect("to have a valid log spec");

    // create a log layer for output to the console and add it to the registry
    let registry = registry.with(console_layer_with_filter(console_filter));

    // create a filter for the log message level in file output. Since the console_filter is not copy or clone, we have to create a new one. TODO: allow a different log level for file output.
    let file_filter =
        EnvFilter::try_new(&logging_config.log_spec).expect("to have a valid log spec");

    // create a log layer for output to a file and add it to the registry
    let (file_layer, _fw_drop_guard) = if logging_config.log_to_file {
        let (file_layer, fw_drop_guard) = file_layer_with_filter(
            &logging_config.filename_prefix,
            logging_config.log_directory.as_deref(),
            file_filter,
        );
        (Some(file_layer), Some(fw_drop_guard))
    } else {
        (None, None)
    };
    let registry = registry.with(file_layer);

    registry.init();

    geoengine_services::server::start_server(None).await
}

#[cfg(feature = "pro")]
/// Starts the server.
///
///  # Panics
///  - if the logging configuration is not valid
///
pub async fn start_server() -> Result<()> {
    reroute_gdal_logging();
    let logging_config: config::Logging = get_config_element()?;
    configure_error_report_formatting(&logging_config);

    // get a new tracing subscriber registry to add all log and tracing layers to
    let registry = tracing_subscriber::Registry::default();

    // create a filter for the log message level in console output
    let console_filter =
        EnvFilter::try_new(&logging_config.log_spec).expect("to have a valid log spec");

    // create a log layer for output to the console and add it to the registry
    let registry = registry.with(console_layer_with_filter(console_filter));

    // create a filter for the log message level in file output. Since the console_filter is not copy or clone, we have to create a new one. TODO: allow a different log level for file output.
    let file_filter =
        EnvFilter::try_new(&logging_config.log_spec).expect("to have a valid log spec");

    // create a log layer for output to a file and add it to the registry
    let (file_layer, _fw_drop_guard) = if logging_config.log_to_file {
        let (file_layer, fw_drop_guard) = file_layer_with_filter(
            &logging_config.filename_prefix,
            logging_config.log_directory.as_deref(),
            file_filter,
        );
        (Some(file_layer), Some(fw_drop_guard))
    } else {
        (None, None)
    };
    let registry = registry.with(file_layer);

    // create a telemetry layer for output to opentelemetry and add it to the registry
    let open_telemetry_config: geoengine_services::pro::util::config::OpenTelemetry =
        get_config_element()?;
    let opentelemetry_layer = if open_telemetry_config.enabled {
        Some(open_telemetry_layer(&open_telemetry_config)?)
    } else {
        None
    };
    let registry = registry.with(opentelemetry_layer);

    // initialize the registry as the global tracing subscriber
    registry.init();

    geoengine_services::pro::server::start_pro_server(None).await
}

#[cfg(feature = "pro")]
fn open_telemetry_layer<S>(
    open_telemetry_config: &geoengine_services::pro::util::config::OpenTelemetry,
) -> Result<
    tracing_opentelemetry::OpenTelemetryLayer<
        S,
        impl opentelemetry::trace::Tracer + tracing_opentelemetry::PreSampledTracer,
    >,
>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(open_telemetry_config.endpoint.to_string());
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(opentelemetry_sdk::trace::Config::default().with_resource(
            opentelemetry_sdk::Resource::new(vec![opentelemetry::KeyValue::new(
                "service.name",
                "Geo Engine",
            )]),
        ))
        .install_simple()?
        .tracer("Geo Engine");
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    Ok(opentelemetry)
    // Ok(OpenTelemetryTracingBridge::new(tracer))
}

fn console_layer_with_filter<S, F: Filter<S> + 'static>(filter: F) -> impl Layer<S>
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .pretty()
        .with_file(false)
        .with_target(true)
        .with_ansi(true)
        .with_writer(std::io::stderr)
        .with_filter(filter)
}

// we use a custom formatter because there are still format flags within spans even when `with_ansi` is false due to bug: https://github.com/tokio-rs/tracing/issues/1817
struct FileFormatterWorkaround(DefaultFields);

impl<'writer> FormatFields<'writer> for FileFormatterWorkaround {
    fn format_fields<R: RecordFields>(
        &self,
        writer: Writer<'writer>,
        fields: R,
    ) -> core::fmt::Result {
        self.0.format_fields(writer, fields)
    }
}

fn file_layer_with_filter<S, F: Filter<S> + 'static>(
    filename_prefix: &str,
    log_directory: Option<&str>,
    filter: F,
) -> (impl Layer<S>, FileLogWriterHandle)
where
    S: Subscriber,
    for<'a> S: LookupSpan<'a>,
{
    let mut file_spec = FileSpec::default().basename(filename_prefix);

    if let Some(dir) = log_directory {
        file_spec = file_spec.directory(dir);
    }

    // TODO: use local time instead of UTC
    // On Unix, using local time implies using an unsound feature: https://docs.rs/flexi_logger/latest/flexi_logger/error_info/index.html#time
    // Thus, we use UTC time instead.
    flexi_logger::DeferredNow::force_utc();

    let (file_writer, fw_handle) = FileLogWriter::builder(file_spec)
        .write_mode(WriteMode::Async)
        .append()
        .rotate(
            Criterion::Age(Age::Day),
            Naming::Timestamps,
            Cleanup::KeepLogFiles(7),
        )
        .try_build_with_handle()
        .expect("file log writer has to be created successfully");

    let layer = tracing_subscriber::fmt::layer()
        .with_file(false)
        .with_target(true)
        // we use a custom formatter because there are still format flags within spans even when `with_ansi` is false due to bug: https://github.com/tokio-rs/tracing/issues/1817
        .fmt_fields(FileFormatterWorkaround(DefaultFields::default()))
        .with_ansi(false)
        .with_writer(move || file_writer.clone())
        .with_filter(filter);
    (layer, fw_handle)
}

/// We install a GDAL error handler that logs all messages with our log macros.
fn reroute_gdal_logging() {
    gdal::config::set_error_handler(|error_type, error_num, error_msg| {
        let target = "GDAL";
        match error_type {
            gdal::errors::CplErrType::None => {
                // should never log anything
                log::info!(target: target, "GDAL None {}: {}", error_num, error_msg);
            }
            gdal::errors::CplErrType::Debug => {
                log::debug!(target: target, "GDAL Debug {}: {}", error_num, error_msg);
            }
            gdal::errors::CplErrType::Warning => {
                log::warn!(target: target, "GDAL Warning {}: {}", error_num, error_msg);
            }
            gdal::errors::CplErrType::Failure => {
                log::error!(target: target, "GDAL Failure {}: {}", error_num, error_msg);
            }
            gdal::errors::CplErrType::Fatal => {
                log::error!(target: target, "GDAL Fatal {}: {}", error_num, error_msg);
            }
        };
    });
}

fn configure_error_report_formatting(logging_config: &config::Logging) {
    if logging_config.raw_error_messages {
        // there is no way to configure snafu::Report other than through env variables
        std::env::set_var("SNAFU_RAW_ERROR_MESSAGES", "1");
    }
}
