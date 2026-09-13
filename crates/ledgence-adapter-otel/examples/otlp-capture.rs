//! Local acceptance receiver: decode real OTLP HTTP/protobuf into JSON lines.
//! This is a verification fixture, not a production OpenTelemetry Collector.
use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
};
use prost::Message;
use serde_json::json;
use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let address = arguments.next().unwrap_or_else(|| "127.0.0.1:4318".into());
    if arguments.next().is_some() {
        return Err("usage: otlp-capture [127.0.0.1:PORT]".into());
    }
    let listener = TcpListener::bind(address)?;
    eprintln!(
        "OTLP_CAPTURE_ENDPOINT=http://{}/v1/traces",
        listener.local_addr()?
    );
    let mut output = io::BufWriter::new(io::stdout().lock());
    for connection in listener.incoming() {
        let mut connection = connection?;
        match receive(&mut connection) {
            Ok(request) => {
                for resource in request.resource_spans {
                    for scope in resource.scope_spans {
                        for span in scope.spans {
                            serde_json::to_writer(
                                &mut output,
                                &json!({
                                    "resource": resource.resource.as_ref().map(|value| attributes(&value.attributes)).unwrap_or_default(),
                                    "scope": scope.scope.as_ref().map(|value| value.name.as_str()),
                                    "name": span.name, "kind": span.kind,
                                    "trace_id": hex(&span.trace_id), "span_id": hex(&span.span_id), "parent_span_id": hex(&span.parent_span_id),
                                    "trace_state": span.trace_state, "flags": span.flags,
                                    "start_time_unix_nano": span.start_time_unix_nano.to_string(), "end_time_unix_nano": span.end_time_unix_nano.to_string(),
                                    "attributes": attributes(&span.attributes),
                                    "links": span.links.iter().map(|link| json!({"trace_id":hex(&link.trace_id), "span_id":hex(&link.span_id), "attributes":attributes(&link.attributes)})).collect::<Vec<_>>(),
                                    "status": span.status.map(|status| json!({"code":status.code,"message":status.message})),
                                    "dropped_attributes_count": span.dropped_attributes_count,
                                }),
                            )?;
                            output.write_all(b"\n")?;
                        }
                    }
                }
                output.flush()?;
                connection.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: application/x-protobuf\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")?;
            }
            Err(error) => {
                eprintln!("OTLP capture rejected a request: {error}");
                let _ = connection.write_all(
                    b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                );
            }
        }
    }
    Ok(())
}
fn receive(
    stream: &mut TcpStream,
) -> Result<ExportTraceServiceRequest, Box<dyn std::error::Error>> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.by_ref().take(8193).read_line(&mut line)?;
    if line != "POST /v1/traces HTTP/1.1\r\n" {
        return Err("expected POST /v1/traces".into());
    }
    let mut length = None;
    let mut content_type = None;
    let mut total = line.len();
    loop {
        line.clear();
        reader.by_ref().take(8193).read_line(&mut line)?;
        total += line.len();
        if line.len() > 8192 || total > 16384 || line.is_empty() {
            return Err("invalid/oversized HTTP headers".into());
        }
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            match name.to_ascii_lowercase().as_str() {
                "content-length" => {
                    if length.is_some() {
                        return Err("duplicate content length".into());
                    }
                    length = Some(value.trim().parse::<usize>()?);
                }
                "content-type" => content_type = Some(value.trim().to_owned()),
                "transfer-encoding" => {
                    return Err("chunked requests are not supported by this capture fixture".into());
                }
                _ => {}
            }
        }
    }
    if content_type.as_deref() != Some("application/x-protobuf") {
        return Err("expected OTLP protobuf".into());
    }
    let length = length.ok_or("missing content length")?;
    if length > 32 * 1024 * 1024 {
        return Err("OTLP batch exceeds capture bound".into());
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok(ExportTraceServiceRequest::decode(body.as_slice())?)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn attributes(values: &[KeyValue]) -> serde_json::Map<String, serde_json::Value> {
    values
        .iter()
        .map(|attribute| (attribute.key.clone(), value(attribute.value.as_ref())))
        .collect()
}
fn value(value: Option<&AnyValue>) -> serde_json::Value {
    match value.and_then(|value| value.value.as_ref()) {
        Some(Value::StringValue(value)) => json!(value),
        Some(Value::BoolValue(value)) => json!(value),
        Some(Value::IntValue(value)) => json!(value),
        Some(Value::DoubleValue(value)) => json!(value),
        Some(Value::ArrayValue(array)) => json!(
            array
                .values
                .iter()
                .map(|item| self::value(Some(item)))
                .collect::<Vec<_>>()
        ),
        Some(Value::KvlistValue(values)) => json!(attributes(&values.values)),
        Some(Value::BytesValue(bytes)) => json!(hex(bytes)),
        Some(Value::StringValueStrindex(index)) => json!({"string_table_index":index}),
        None => serde_json::Value::Null,
    }
}
