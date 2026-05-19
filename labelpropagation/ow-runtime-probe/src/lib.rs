use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use burst_communication_middleware::{Middleware, MiddlewareActorHandle};
use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use serde_json::{Error, Value};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Input {
    mode: String,
    partitions: u32,
    granularity: u32,
    payload_bytes: Option<usize>,
    iterations: Option<u32>,
    root_worker: Option<u32>,
    peer_worker: Option<u32>,
    bucket: Option<String>,
    key_prefix: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_session_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Output {
    worker_id: u32,
    mode: String,
    payload_bytes: usize,
    iterations: u32,
    timestamps: Vec<Timestamp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes_read: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Timestamp {
    key: String,
    value: String,
}

fn timestamp(key: &str) -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    Timestamp {
        key: key.to_string(),
        value: now.to_string(),
    }
}

fn payload(worker_id: u32, size: usize) -> Bytes {
    Bytes::from(vec![(worker_id % 251) as u8; size])
}

fn mw<T>(
    result: std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>,
) -> Result<T> {
    result.map_err(|err| anyhow::anyhow!(err.to_string()))
}

fn run_startup_probe(
    input: &Input,
    middleware: &MiddlewareActorHandle<Bytes>,
) -> Result<Output> {
    let mut timestamps = vec![timestamp("worker_start"), timestamp("probe_start"), timestamp("probe_end"), timestamp("worker_end")];
    Ok(Output {
        worker_id: middleware.info.worker_id,
        mode: input.mode.clone(),
        payload_bytes: input.payload_bytes.unwrap_or(0),
        iterations: input.iterations.unwrap_or(1),
        timestamps: std::mem::take(&mut timestamps),
        bytes_read: None,
        summary: None,
    })
}

fn run_broadcast_probe(
    input: &Input,
    middleware: &MiddlewareActorHandle<Bytes>,
) -> Result<Output> {
    let root = input.root_worker.unwrap_or(0);
    let payload_size = input.payload_bytes.unwrap_or(1 << 20);
    let iterations = input.iterations.unwrap_or(1);
    let worker_id = middleware.info.worker_id;
    let mut timestamps = vec![timestamp("worker_start")];

    for iter in 0..iterations {
        timestamps.push(timestamp(&format!("iter_{}_start", iter)));
        if worker_id == root {
            let _ = mw(middleware.broadcast(Some(payload(worker_id, payload_size)), root))?;
        } else {
            let _ = mw(middleware.broadcast(None, root))?;
        }
        timestamps.push(timestamp(&format!("iter_{}_broadcast_end", iter)));
    }
    timestamps.push(timestamp("worker_end"));

    Ok(Output {
        worker_id,
        mode: input.mode.clone(),
        payload_bytes: payload_size,
        iterations,
        timestamps,
        bytes_read: None,
        summary: if worker_id == root {
            Some(format!(
                "broadcast payload={} bytes iterations={}",
                payload_size, iterations
            ))
        } else {
            None
        },
    })
}

fn run_all_to_all_probe(
    input: &Input,
    middleware: &MiddlewareActorHandle<Bytes>,
) -> Result<Output> {
    let payload_size = input.payload_bytes.unwrap_or(1 << 20);
    let iterations = input.iterations.unwrap_or(1);
    let worker_id = middleware.info.worker_id;
    let burst_size = middleware.info.burst_size;
    let mut timestamps = vec![timestamp("worker_start")];

    for iter in 0..iterations {
        timestamps.push(timestamp(&format!("iter_{}_start", iter)));
        let mut outbound = Vec::with_capacity(burst_size as usize);
        for dest in 0..burst_size {
            outbound.push(payload(worker_id.saturating_add(dest), payload_size));
        }
        let _ = mw(middleware.all_to_all(outbound))?;
        timestamps.push(timestamp(&format!("iter_{}_all_to_all_end", iter)));
    }
    timestamps.push(timestamp("worker_end"));

    Ok(Output {
        worker_id,
        mode: input.mode.clone(),
        payload_bytes: payload_size,
        iterations,
        timestamps,
        bytes_read: None,
        summary: if worker_id == 0 {
            Some(format!(
                "all_to_all payload={} bytes iterations={}",
                payload_size, iterations
            ))
        } else {
            None
        },
    })
}

fn run_ptp_probe(
    input: &Input,
    middleware: &MiddlewareActorHandle<Bytes>,
) -> Result<Output> {
    let worker_id = middleware.info.worker_id;
    let payload_size = input.payload_bytes.unwrap_or(1 << 20);
    let iterations = input.iterations.unwrap_or(16);
    let root = input.root_worker.unwrap_or(0);
    let peer = input.peer_worker.unwrap_or(1);
    let mut timestamps = vec![timestamp("worker_start")];

    if worker_id == root {
        timestamps.push(timestamp("ptp_start"));
        for _ in 0..iterations {
            mw(middleware.send(peer, payload(root, payload_size)))?;
        }
        let _ = mw(middleware.recv(peer))?;
        timestamps.push(timestamp("ptp_end"));
    } else if worker_id == peer {
        timestamps.push(timestamp("ptp_start"));
        for _ in 0..iterations {
            let _ = mw(middleware.recv(root))?;
        }
        mw(middleware.send(root, Bytes::from_static(b"ack")))?;
        timestamps.push(timestamp("ptp_end"));
    }

    timestamps.push(timestamp("worker_end"));
    Ok(Output {
        worker_id,
        mode: input.mode.clone(),
        payload_bytes: payload_size,
        iterations,
        timestamps,
        bytes_read: None,
        summary: if worker_id == root {
            Some(format!(
                "ptp payload={} bytes iterations={} root={} peer={}",
                payload_size, iterations, root, peer
            ))
        } else {
            None
        },
    })
}

fn run_ptp_pairs_probe(
    input: &Input,
    middleware: &MiddlewareActorHandle<Bytes>,
) -> Result<Output> {
    let worker_id = middleware.info.worker_id;
    let payload_size = input.payload_bytes.unwrap_or(1 << 20);
    let iterations = input.iterations.unwrap_or(16);
    let burst_size = middleware.info.burst_size;
    let mut timestamps = vec![timestamp("worker_start")];

    if worker_id % 2 == 0 && worker_id + 1 < burst_size {
        let peer = worker_id + 1;
        timestamps.push(timestamp("ptp_start"));
        for _ in 0..iterations {
            mw(middleware.send(peer, payload(worker_id, payload_size)))?;
        }
        let _ = mw(middleware.recv(peer))?;
        timestamps.push(timestamp("ptp_end"));
    } else if worker_id % 2 == 1 {
        let root = worker_id - 1;
        timestamps.push(timestamp("ptp_start"));
        for _ in 0..iterations {
            let _ = mw(middleware.recv(root))?;
        }
        mw(middleware.send(root, Bytes::from_static(b"ack")))?;
        timestamps.push(timestamp("ptp_end"));
    }

    timestamps.push(timestamp("worker_end"));
    Ok(Output {
        worker_id,
        mode: input.mode.clone(),
        payload_bytes: payload_size,
        iterations,
        timestamps,
        bytes_read: None,
        summary: if worker_id == 0 {
            Some(format!(
                "ptp_pairs payload={} bytes iterations={} workers={}",
                payload_size, iterations, burst_size
            ))
        } else {
            None
        },
    })
}

async fn load_partition_bytes(input: &Input, s3_client: &S3Client, worker_id: u32) -> Result<usize> {
    let bucket = input
        .bucket
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("load probe requires bucket"))?;
    let key_prefix = input
        .key_prefix
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("load probe requires key_prefix"))?;
    let key = format!("{}/part-{:05}", key_prefix, worker_id);
    let output = s3_client
        .get_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await?;
    let bytes = output.body.collect().await?.into_bytes();
    Ok(bytes.len())
}

fn run_load_probe(
    input: &Input,
    middleware: &MiddlewareActorHandle<Bytes>,
) -> Result<Output> {
    let worker_id = middleware.info.worker_id;
    let credentials_provider = Credentials::from_keys(
        input.aws_access_key_id
            .clone()
            .unwrap_or_else(|| "minioadmin".to_string()),
        input.aws_secret_access_key
            .clone()
            .unwrap_or_else(|| "minioadmin".to_string()),
        input.aws_session_token.clone(),
    );
    let config = match input.endpoint.clone() {
        Some(endpoint) => aws_sdk_s3::config::Builder::new()
            .endpoint_url(endpoint)
            .credentials_provider(credentials_provider)
            .region(Region::new(
                input.region.clone().unwrap_or_else(|| "us-east-1".to_string()),
            ))
            .force_path_style(true)
            .build(),
        None => aws_sdk_s3::config::Builder::new()
            .credentials_provider(credentials_provider)
            .region(Region::new(
                input.region.clone().unwrap_or_else(|| "us-east-1".to_string()),
            ))
            .build(),
    };
    let s3_client = S3Client::from_conf(config);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut timestamps = vec![timestamp("worker_start")];
    timestamps.push(timestamp("get_input"));
    let bytes_read = rt.block_on(load_partition_bytes(input, &s3_client, worker_id))?;
    timestamps.push(timestamp("get_input_end"));
    timestamps.push(timestamp("worker_end"));

    Ok(Output {
        worker_id,
        mode: input.mode.clone(),
        payload_bytes: 0,
        iterations: 1,
        timestamps,
        bytes_read: Some(bytes_read),
        summary: if worker_id == 0 {
            Some(format!(
                "load key_prefix={} workers={}",
                input.key_prefix.clone().unwrap_or_default(),
                middleware.info.burst_size
            ))
        } else {
            None
        },
    })
}

fn run_probe(input: &Input, middleware: &MiddlewareActorHandle<Bytes>) -> Result<Output> {
    match input.mode.as_str() {
        "startup" => run_startup_probe(input, middleware),
        "load" => run_load_probe(input, middleware),
        "broadcast" => run_broadcast_probe(input, middleware),
        "all_to_all" => run_all_to_all_probe(input, middleware),
        "ptp" => run_ptp_probe(input, middleware),
        "ptp_pairs" => run_ptp_pairs_probe(input, middleware),
        other => Err(anyhow::anyhow!("unsupported probe mode: {}", other)),
    }
}

pub fn main(args: Value, burst_middleware: Middleware<Bytes>) -> Result<Value, Error> {
    let input: Input = serde_json::from_value(args)?;
    if input.partitions % input.granularity != 0 {
        panic!(
            "ERROR: partitions ({}) must be divisible by granularity ({})",
            input.partitions, input.granularity
        );
    }
    let handle = burst_middleware.get_actor_handle();
    let result = run_probe(&input, &handle).map_err(|err| serde_json::Error::io(std::io::Error::other(err.to_string())))?;
    serde_json::to_value(result)
}
