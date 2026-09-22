use super::*;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    metrics::v1::{
        HistogramDataPoint, NumberDataPoint, metric::Data, number_data_point::Value as Number,
    },
};

pub fn emit(
    request: ExportMetricsServiceRequest,
    output: &mut impl Write,
) -> Result<(), Box<dyn std::error::Error>> {
    for resource in request.resource_metrics {
        for scope in resource.scope_metrics {
            for metric in scope.metrics {
                let (kind, temporality, monotonic, points) = match metric.data {
                    Some(Data::Sum(sum)) => (
                        "sum",
                        sum.aggregation_temporality,
                        Some(sum.is_monotonic),
                        sum.data_points
                            .into_iter()
                            .map(sum_point)
                            .collect::<Vec<_>>(),
                    ),
                    Some(Data::Histogram(histogram)) => (
                        "histogram",
                        histogram.aggregation_temporality,
                        None,
                        histogram
                            .data_points
                            .into_iter()
                            .map(histogram_point)
                            .collect(),
                    ),
                    _ => return Err("unsupported metric representation".into()),
                };
                serde_json::to_writer(
                    &mut *output,
                    &json!({
                        "signal": "metrics",
                        "resource": resource.resource.as_ref().map(|r| attributes(&r.attributes)).unwrap_or_default(),
                        "scope": scope.scope.as_ref().map(|s| s.name.as_str()),
                        "name": metric.name,
                        "unit": metric.unit,
                        "kind": kind,
                        "temporality": temporality,
                        "monotonic": monotonic,
                        "points": points,
                    }),
                )?;
                output.write_all(b"\n")?;
            }
        }
    }
    Ok(())
}

fn sum_point(point: NumberDataPoint) -> serde_json::Value {
    json!({
        "attributes": attributes(&point.attributes),
        "value": point.value.map(|value| match value {
            Number::AsDouble(value) => json!(value),
            Number::AsInt(value) => json!(value),
        }),
        "start_time_unix_nano": point.start_time_unix_nano.to_string(),
        "time_unix_nano": point.time_unix_nano.to_string(),
    })
}

fn histogram_point(point: HistogramDataPoint) -> serde_json::Value {
    json!({
        "attributes": attributes(&point.attributes),
        "count": point.count,
        "sum": point.sum,
        "min": point.min,
        "max": point.max,
        "bounds": point.explicit_bounds,
        "buckets": point.bucket_counts,
        "start_time_unix_nano": point.start_time_unix_nano.to_string(),
        "time_unix_nano": point.time_unix_nano.to_string(),
    })
}
