use std::{
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;

#[derive(Clone)]
struct AppState {
    click_count: Arc<AtomicU64>,
    updates: broadcast::Sender<u64>,
}

#[derive(Deserialize)]
struct ClientMessage {
    event: String,
}

#[derive(Serialize)]
struct ServerMessage {
    event: &'static str,
    count: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = env::var("PORT")
        .unwrap_or_else(|_| "3000".to_owned())
        .parse::<u16>()?;
    let (updates, _) = broadcast::channel(100);
    let state = AppState {
        click_count: Arc::new(AtomicU64::new(0)),
        updates,
    };
    let static_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .fallback_service(ServeDir::new(static_dir).append_index_html_on_directories(true))
        .with_state(state);

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("Server running on port {port}");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut updates = state.updates.subscribe();
    let initial_count = state.click_count.load(Ordering::Relaxed);

    if send_count(&mut sender, initial_count).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<ClientMessage>(text.as_str()) {
                            Ok(message) if message.event == "cookieClick" => {
                                let count = state.click_count.fetch_add(1, Ordering::Relaxed) + 1;
                                if state.updates.send(count).is_err() {
                                    eprintln!("Unable to broadcast updated click count: no active subscribers");
                                }
                            }
                            Ok(_) => {}
                            Err(error) => eprintln!("Ignoring invalid WebSocket message: {error}"),
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if sender.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        eprintln!("WebSocket connection error: {error}");
                        break;
                    }
                }
            }
            update = updates.recv() => {
                match update {
                    Ok(count) => {
                        if send_count(&mut sender, count).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let count = state.click_count.load(Ordering::Relaxed);
                        if send_count(&mut sender, count).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

async fn send_count(
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    count: u64,
) -> Result<(), axum::Error> {
    let message = ServerMessage {
        event: "updateCount",
        count,
    };
    let text =
        serde_json::to_string(&message).expect("serializing a click-count update should not fail");
    sender.send(Message::Text(text.into())).await
}
