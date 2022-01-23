use libc::c_int;
use mini_pd::{AddressMap, Config, Server};
use nix::sys::signal::{SIGHUP, SIGINT, SIGTERM, SIGUSR1, SIGUSR2};
use parking_lot::Mutex;
use signal::trap::Trap;
use slog::{debug, info, Logger};
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::Severity;
use sloggers::Build;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempdir::TempDir;

use clap::{crate_authors, App, Arg};
//use tracing_subscriber::{layer::SubscriberExt, registry::Registry};
//use tracing_libatrace as tracing_atrace;

fn main() {
    //    let layer = tracing_atrace::layer().unwrap().with_data_field(Option::Some("data".to_string()));
    //    let subscriber = Registry::default().with(layer);
    //    tracing::subscriber::set_global_default(subscriber).unwrap();
    let matches = App::new("mini-pd")
        .about("A mini-pd by Rust and Raft")
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .value_name("FILE")
                .help("Set the configuration file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("data-dir")
                .long("data-dir")
                .short("s")
                .alias("store")
                .takes_value(true)
                .value_name("PATH")
                .help("Set the directory used to store data"),
        )
        .arg(
            Arg::with_name("-peer-urls")
                .long("peer-urls")
                .aliases(&["pd", "pd-peers"])
                .takes_value(true)
                .value_name("PEER_URL")
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .help("Sets Peer urls")
                .long_help("Set the peer endpoint to use. Use `,` to separate multiple peers"),
        )
        .arg(
            Arg::with_name("my-id")
                .short("my-id")
                .help("my peerid, which is number of peer-urls, begin with 1")
                .takes_value(true),
        )
        .get_matches();

    let mut builder = TerminalLoggerBuilder::new();
    if env::var("SLOG").is_ok() {
        builder.level(Severity::Trace);
    } else {
        builder.level(Severity::Debug);
    }

    builder.destination(Destination::Stderr);
    let logger = builder.build().unwrap();

    let mut peers = Vec::default();
    let mut map = Arc::new(Mutex::new(HashMap::default()));
    let data_dir = matches.value_of("data-dir").unwrap_or("pd").to_string();
    if let Some(peer_url_vec) = matches.values_of("-peer-urls") {
        let peer_urls: Vec<String> = peer_url_vec.map(ToOwned::to_owned).collect();
        let initial_count = peer_urls.len();
        for id in 1..=initial_count {
            peers.push(id as u64);
            map.lock().insert(id as u64, peer_urls[id - 1].clone());
            debug!(logger, "peer id:{}, url:{:#?}", id, peer_urls[id - 1]);
        }
    } else {
        let id = 1;
        peers.push(id as u64);
        map.lock().insert(id as u64, "127.0.0.1:2379".to_string());
        debug!(logger, "default peer id:{}", id);
    }
    let my_id = matches
        .value_of("my-id")
        .unwrap_or("1")
        .parse::<u64>()
        .unwrap();
    let my_addr = map
        .lock()
        .get(&my_id)
        .unwrap_or(&"127.0.0.1:2379".to_string())
        .to_string();
    let mut config = Config::default();
    config.my_id = my_id;
    config.address = my_addr.clone();
    config.advertise_address = my_addr.clone();
    config.data_dir = Path::new(&data_dir).to_path_buf();
    config.initial_peers = peers.clone();
    config.initial_address_book.insert(my_id, my_addr.clone());
    config.raft_election_ticks = 5;
    config.raft_heartbeat_ticks = 1;
    let mut server = Server::new(map.clone(), config, logger.clone());
    info!(logger, "after new server, will start with:{:#?}", my_addr);
    server.start();
    info!(logger, "after server start with:{:#?}", my_addr);
    let trap = Trap::trap(&[SIGTERM, SIGINT, SIGHUP, SIGUSR1, SIGUSR2]);
    for sig in trap {
        match sig {
            SIGTERM | SIGINT | SIGHUP => {
                // info!("receive signal {}, stopping server...", sig as c_int);
                break;
            }
            SIGUSR1 => {
                // Use SIGUSR1 to log metrics.
            }
            // TODO: handle more signal
            _ => unreachable!(),
        }
    }
    info!(logger, "after trap, will server shutdown");
    server.shutdown();
    info!(logger, "server shutdown");
}
