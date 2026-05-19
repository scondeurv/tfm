use actions::{main as ow_main, LabelsMessage};
use burst_communication_middleware::{
    BurstMiddleware, BurstOptions, Middleware, RedisListImpl, RedisListOptions, TokioChannelImpl,
    TokioChannelOptions,
};
use clap::Parser;
use log::info;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    thread,
};

/// Manual harness for running the action against a local Burst middleware setup.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "labelpropagation")]
    burst_id: String,

    #[arg(long)]
    granularity: u32,

    #[arg(long)]
    burst_size: u32,

    #[arg(long)]
    group_id: u32,

    #[arg(short, long, default_value = "input_payload.json")]
    input_json_params: String,

    #[arg(short, long)]
    redis_url: String,

    #[arg(short, long, default_value = "true")]
    enable_chunking: bool,

    #[arg(short, long, default_value = "1048576")]
    message_chunk_size: usize,
}

fn main() {
    env_logger::init();

    let args = Args::parse();

    let input_json_file = File::open(args.input_json_params).unwrap();
    let params: Vec<Value> = serde_json::from_reader(input_json_file).unwrap();

    if args.burst_size % args.granularity != 0 {
        panic!(
            "BURST_SIZE {} must be divisible by GRANULARITY {}",
            args.burst_size, args.granularity
        );
    }

    let num_groups = args.burst_size / args.granularity;
    println!("num_groups: {}", num_groups);
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let group_ranges = (0..num_groups)
        .map(|group_id| {
            (
                group_id.to_string(),
                ((args.granularity * group_id)..((args.granularity * group_id) + args.granularity))
                    .collect(),
            )
        })
        .collect::<HashMap<String, HashSet<u32>>>();

    let burst_options = BurstOptions::new(
        args.burst_size,
        group_ranges.clone(),
        args.group_id.to_string(),
    )
    .burst_id(args.burst_id.clone())
    .enable_message_chunking(args.enable_chunking)
    .message_chunk_size(args.message_chunk_size)
    .build();
    let channel_options = TokioChannelOptions::new()
        .broadcast_channel_size(256)
        .build();
    let backend_options = RedisListOptions::new(args.redis_url.clone()).build();

    let fut = BurstMiddleware::create_proxies::<TokioChannelImpl, RedisListImpl, _, _>(
        burst_options,
        channel_options,
        backend_options,
    );
    let proxies = tokio_runtime.block_on(fut).unwrap();

    let actors = proxies
        .into_iter()
        .map(|(worker_id, middleware)| {
            (
                worker_id,
                Middleware::new(middleware, tokio_runtime.handle().clone()),
            )
        })
        .collect::<HashMap<u32, Middleware<LabelsMessage>>>();

    let mut actors_vec = actors.into_iter().collect::<Vec<_>>();
    // sort proxies by worker_id so that the order of the workers and the ordered params is correct
    actors_vec.sort_by(|(a, _), (b, _)| a.cmp(b));

    assert!(actors_vec.len() == params.len());

    let threads = actors_vec
        .into_iter()
        .zip(params)
        .map(|(proxies, param)| {
            thread::spawn(move || {
                let (worker_id, proxy) = proxies;
                info!("thread start: id={}", worker_id);
                let result = ow_main(param, proxy);
                info!("thread end: id={}", worker_id);
                result
            })
        })
        .collect::<Vec<_>>();

    let mut results = Vec::with_capacity(threads.len());
    for thread in threads {
        let worker_result = thread.join().unwrap().unwrap();
        results.push(worker_result);
    }

    let output_filename = format!("output_{}_group-{}.json", args.burst_id, args.group_id);
    let output_file = File::create(output_filename).unwrap();
    serde_json::to_writer(output_file, &results).unwrap();
}
