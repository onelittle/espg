use std::{
    collections::VecDeque,
    sync::{Arc, atomic::AtomicUsize},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt as _},
    net::UnixListener,
    sync::Mutex,
};

static USAGE: AtomicUsize = AtomicUsize::new(0);
static PEAK_USAGE: AtomicUsize = AtomicUsize::new(0);
static TOTAL_WAIT: AtomicUsize = AtomicUsize::new(0);

fn increment_usage() {
    let current = USAGE.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let peak = PEAK_USAGE.load(std::sync::atomic::Ordering::Relaxed);
    if current > peak {
        #[cfg(debug_assertions)]
        eprintln!("Peak usage: {}", current);
        PEAK_USAGE.store(current, std::sync::atomic::Ordering::Relaxed);
    }
}

fn decrement_usage() -> usize {
    USAGE.fetch_sub(1, std::sync::atomic::Ordering::Relaxed)
}

#[tokio::main]
async fn main() {
    // Open a Unix domain socket listener on {cwd}/tmp/test_manager.sock
    let path = std::env::current_dir()
        .unwrap()
        .join("tmp")
        .join("test_manager.sock");

    let listener = UnixListener::bind(&path).unwrap();

    let mut databases: VecDeque<String> = VecDeque::new();
    let max_count = std::env::var("DATABASE_COUNT")
        .map(|n| n.parse::<usize>().unwrap())
        .unwrap_or(8);
    let db_prefix = std::env::var("DATABASE_PREFIX").expect("DATABASE_PREFIX must be set");
    for n in 0..max_count {
        databases.push_back(format!("{}{}", db_prefix, n));
    }

    let databases = Arc::new(Mutex::new(databases));

    tokio::spawn(async move {
        eprintln!("Listening on {:?}", path);
        loop {
            match listener.accept().await {
                Ok((mut stream, addr)) => {
                    let databases = databases.clone();
                    tokio::spawn(async move {
                        #[cfg(debug_assertions)]
                        eprintln!("New connection from {:?}", addr);
                        #[cfg(debug_assertions)]
                        eprintln!("Assigning database...");
                        let name = {
                            loop {
                                let mut dbs = databases.lock().await;
                                if let Some(name) = dbs.pop_front() {
                                    increment_usage();
                                    break name.clone();
                                }
                                drop(dbs);
                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                TOTAL_WAIT.fetch_add(10, std::sync::atomic::Ordering::Relaxed);
                            }
                        };
                        let instant = std::time::Instant::now();
                        // Respont to the client OK:{db_name} or EMPTY:No databases available
                        #[cfg(debug_assertions)]
                        eprintln!("Assigned database: {:?}", name);
                        if let Err(e) = stream.write_all(format!("OK:{}", name).as_bytes()).await {
                            #[cfg(debug_assertions)]
                            eprintln!("Failed to write to stream: {}", e);
                        }
                        stream.flush().await.unwrap();

                        let mut buffer = [0; 1024];
                        if let Ok(0) = stream.read(&mut buffer).await {
                            #[cfg(debug_assertions)]
                            eprintln!("Client disconnected");
                            #[cfg(debug_assertions)]
                            eprintln!(
                                "Releasing database: {} after {}ms usage",
                                name,
                                instant.elapsed().as_millis()
                            );
                            let mut dbs = databases.lock().await;
                            dbs.push_back(name);
                            decrement_usage();
                        }
                    });
                }
                Err(_) => { /* connection failed */ }
            }
        }
    });

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            eprintln!("Received shutdown signal, shutting down...");
            eprintln!(
                "Peak usage: {}",
                PEAK_USAGE.load(std::sync::atomic::Ordering::Relaxed)
            );
            eprintln!(
                "Total wait time: {}ms",
                TOTAL_WAIT.load(std::sync::atomic::Ordering::Relaxed)
            );
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
        }
    }
}
