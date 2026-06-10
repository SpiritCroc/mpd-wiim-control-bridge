use anyhow::Context;
use clap::Parser;
use log::{debug, error, info, trace, warn};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 6602)]
    port: u16,
    #[arg(short, long, default_value_t = String::from("0.0.0.0"))]
    bind_address: String,
    #[arg(long)]
    wiim_host: String,
    #[arg(long, default_value_t = String::from("https"))]
    scheme: String,
    #[arg(long, default_value_t = 1000)]
    poll_ms: u64,
}

#[derive(Debug)]
enum Command {
    Play,
    Pause,
    Stop,
    Next,
    Prev,
    Seek(u64),
    SetVolume(u8),
    ChangeVolume(i8),
    SetRepeat(bool),
    SetRandom(bool),
    SetSingle(SingleMode),
    SetOutput(OutputMode),
    PlayPreset(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaybackStatus {
    Play,
    Pause,
    Stop,
    Loading,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct LoopState {
    repeat: bool,
    random: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum SingleMode {
    #[default]
    Off,
    On,
    OneShot,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct PlaybackOptions {
    single: SingleMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Spdif,
    Aux,
    Coax,
}

impl OutputMode {
    fn id(self) -> u8 {
        match self {
            OutputMode::Spdif => 1,
            OutputMode::Aux => 2,
            OutputMode::Coax => 3,
        }
    }

    fn name(self) -> &'static str {
        match self {
            OutputMode::Spdif => "SPDIF",
            OutputMode::Aux => "AUX",
            OutputMode::Coax => "COAX",
        }
    }
}

const OUTPUT_MODES: [OutputMode; 3] = [OutputMode::Spdif, OutputMode::Aux, OutputMode::Coax];
const WIIM_READ_FAILURE_CLEAR_THRESHOLD: u32 = 5;

#[derive(Debug, Clone, PartialEq)]
struct PlayerState {
    playback_status: PlaybackStatus,
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
    duration: Option<f32>,
    elapsed: Option<f32>,
    art_url: Option<String>,
    volume: Option<u8>,
    mute: Option<bool>,
    loop_state: LoopState,
    playlist_len: Option<u32>,
    song_index: Option<u32>,
    playlist_version: u32,
}

#[derive(Debug, Clone, PartialEq)]
struct PlayerStateForIdle {
    playback_status: PlaybackStatus,
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
    art_url: Option<String>,
    loop_state: LoopState,
    single: SingleMode,
    selected_preset: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Preset {
    number: u8,
    name: String,
    source: Option<String>,
    url: Option<String>,
    art_url: Option<String>,
}

struct MpdSharedState {
    player_state: Arc<RwLock<Option<PlayerState>>>,
    output_mode: Arc<RwLock<Option<OutputMode>>>,
    presets: Arc<RwLock<Vec<Preset>>>,
    selected_preset: Arc<RwLock<Option<u8>>>,
    playback_options: Arc<RwLock<PlaybackOptions>>,
}

struct MpdQueryState {
    command_tx: mpsc::Sender<Command>,
    pending_input: Vec<u8>,
    in_command_list: bool,
    in_command_list_ok: bool,
    command_list_ended: bool,
    command_list_count: usize,
    command_list_failed: bool,
    last_idle_player_state: Option<PlayerStateForIdle>,
    last_idle_playlist_state: Option<PlayerStateForIdle>,
    last_idle_mixer_state: Option<(Option<u8>, Option<bool>)>,
    last_idle_output_state: Option<OutputMode>,
    should_close: bool,
}

#[derive(Debug)]
struct MpdCommandError {
    command_str: String,
    message: String,
    mpd_error_code: i8,
}

impl std::error::Error for MpdCommandError {}

impl std::fmt::Display for MpdCommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Command {} failed: {}", self.command_str, self.message)
    }
}

impl MpdCommandError {
    fn new(command: &[u8], message: &str) -> MpdCommandError {
        MpdCommandError {
            command_str: safe_command_print(command).to_string(),
            message: message.to_string(),
            mpd_error_code: 5,
        }
    }
}

#[derive(Clone)]
struct WiimClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Deserialize)]
struct WiimPlayerStatus {
    status: Option<String>,
    curpos: Option<String>,
    totlen: Option<String>,
    vol: Option<String>,
    mute: Option<String>,
    #[serde(rename = "loop")]
    loop_mode: Option<String>,
    plicount: Option<String>,
    plicurr: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WiimMetaResponse {
    #[serde(rename = "metaData")]
    meta_data: Option<Value>,
}

#[derive(Debug, Default)]
struct WiimMetaInfo {
    title: Option<String>,
    artist: Option<String>,
    album: Option<String>,
    art_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WiimOutputModeResponse {
    hardware: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WiimPresetResponse {
    preset_list: Option<Vec<WiimPreset>>,
}

#[derive(Debug, Deserialize)]
struct WiimPreset {
    number: Option<u8>,
    name: Option<String>,
    source: Option<String>,
    url: Option<String>,
    picurl: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    let args = Args::parse();

    let wiim = WiimClient::new(&args.wiim_host, &args.scheme)?;
    let address = format!("{}:{}", args.bind_address, args.port);
    info!("Binding MPD server to {address}...");
    let listener = TcpListener::bind(address).await?;

    let (command_tx, command_rx) = mpsc::channel(16);
    let shared_state = Arc::new(MpdSharedState {
        player_state: Arc::new(RwLock::new(None)),
        output_mode: Arc::new(RwLock::new(None)),
        presets: Arc::new(RwLock::new(Vec::new())),
        selected_preset: Arc::new(RwLock::new(None)),
        playback_options: Arc::new(RwLock::new(PlaybackOptions::default())),
    });

    let server_state = shared_state.clone();
    let server_tx = command_tx.clone();
    tokio::spawn(async move {
        loop {
            let (socket, addr) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("Failed to accept MPD client: {e}");
                    continue;
                }
            };
            info!("Connected client {addr}");
            tokio::spawn(handle_client(
                socket,
                server_tx.clone(),
                server_state.clone(),
            ));
        }
    });

    observe_wiim(
        wiim,
        command_rx,
        shared_state,
        Duration::from_millis(args.poll_ms),
    )
    .await;
    Ok(())
}

impl WiimClient {
    fn new(host: &str, scheme: &str) -> anyhow::Result<Self> {
        let client = Client::builder()
            .danger_accept_invalid_certs(true)
            .timeout(Duration::from_secs(10))
            .build()?;
        let base_url = if host.starts_with("http://") || host.starts_with("https://") {
            host.trim_end_matches('/').to_string()
        } else {
            format!("{}://{}", scheme, host.trim_end_matches('/'))
        };
        Ok(Self { client, base_url })
    }

    async fn command_text(&self, command: &str) -> anyhow::Result<String> {
        let response = self
            .client
            .get(format!("{}/httpapi.asp", self.base_url))
            .query(&[("command", command)])
            .send()
            .await?
            .error_for_status()?;
        Ok(response.text().await?)
    }

    async fn command_json<T: for<'de> Deserialize<'de>>(&self, command: &str) -> anyhow::Result<T> {
        let text = self.command_text(command).await?;
        serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse WiiM response: {text}"))
    }

    async fn get_player_status(&self) -> anyhow::Result<WiimPlayerStatus> {
        self.command_json("getPlayerStatus").await
    }

    async fn get_meta_info(&self) -> anyhow::Result<WiimMetaInfo> {
        let text = self.command_text("getMetaInfo").await?;
        if text.trim() == "Failed" {
            // Normal after reboot, don't be too noisy
            return Ok(WiimMetaInfo::default());
        }
        let response: WiimMetaResponse = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse WiiM response: {text}"))?;
        let Some(meta) = response.meta_data else {
            return Ok(WiimMetaInfo::default());
        };
        Ok(WiimMetaInfo {
            title: json_string(&meta, &["title"]),
            artist: json_string(&meta, &["artist"]),
            album: json_string(&meta, &["album"]),
            art_url: json_string(&meta, &["albumArtURI", "albumArtURI ", "artUrl", "picurl"]),
        })
    }

    async fn get_output_mode(&self) -> anyhow::Result<Option<OutputMode>> {
        let text = self.command_text("getNewAudioOutputHardwareMode").await?;
        if text.trim() == "Failed" {
            return Ok(None);
        }
        let response: WiimOutputModeResponse = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse WiiM response: {text}"))?;
        Ok(response.hardware.as_deref().and_then(parse_output_mode))
    }

    async fn set_output_mode(&self, output_mode: OutputMode) -> anyhow::Result<()> {
        let response = self
            .command_text(&format!("setAudioOutputHardwareMode:{}", output_mode.id()))
            .await?;
        trace!("WiiM output mode command response: {response}");
        Ok(())
    }

    async fn get_presets(&self) -> anyhow::Result<Vec<Preset>> {
        let text = self.command_text("getPresetInfo").await?;
        if text.trim() == "Failed" {
            return Ok(Vec::new());
        }
        let response: WiimPresetResponse = serde_json::from_str(&text)
            .with_context(|| format!("Failed to parse WiiM response: {text}"))?;
        Ok(response
            .preset_list
            .unwrap_or_default()
            .into_iter()
            .filter_map(|preset| {
                let number = preset.number?;
                let name = preset.name.unwrap_or_else(|| format!("Preset {number}"));
                Some(Preset {
                    number,
                    name,
                    source: preset.source,
                    url: preset.url,
                    art_url: preset.picurl,
                })
            })
            .collect())
    }

    async fn send_player_cmd(&self, command: &str) -> anyhow::Result<()> {
        let response = self
            .command_text(&format!("setPlayerCmd:{command}"))
            .await?;
        trace!("WiiM command {command} response: {response}");
        Ok(())
    }
}

async fn observe_wiim(
    wiim: WiimClient,
    mut command_rx: mpsc::Receiver<Command>,
    shared_state: Arc<MpdSharedState>,
    poll_delay: Duration,
) {
    let mut last_emitted_player_state = None;
    let mut last_playlist_identity = None;
    let mut playlist_version = 0u32;
    let mut last_emitted_output_mode = None;
    let mut last_emitted_presets = Vec::new();
    let mut player_state_failures = 0u32;
    let mut output_mode_failures = 0u32;
    let mut preset_failures = 0u32;
    let mut preset_poll_ticks = 0u8;
    loop {
        match timeout(poll_delay, command_rx.recv()).await {
            Ok(Some(command)) => handle_wiim_command(&wiim, command, &shared_state).await,
            Ok(None) => warn!("Command channel closed"),
            Err(_) => trace!("Polling WiiM"),
        }

        let mut player_state = match read_wiim_state(&wiim).await {
            Ok(state) => {
                if player_state_failures > 0 {
                    info!(
                        "Recovered reading WiiM state after {player_state_failures} failed polls"
                    );
                    player_state_failures = 0;
                }
                Some(state)
            }
            Err(e) => {
                player_state_failures += 1;
                if player_state_failures == 1 {
                    warn!("Failed to read WiiM state, keeping last known state: {e}");
                } else if player_state_failures == WIIM_READ_FAILURE_CLEAR_THRESHOLD {
                    warn!(
                        "Failed to read WiiM state {player_state_failures} consecutive times, clearing state: {e}"
                    );
                } else {
                    debug!(
                        "Failed to read WiiM state ({player_state_failures} consecutive failures): {e}"
                    );
                }
                if player_state_failures >= WIIM_READ_FAILURE_CLEAR_THRESHOLD {
                    None
                } else {
                    last_emitted_player_state.clone()
                }
            }
        };
        if let Some(player_state) = player_state.as_mut() {
            let current_identity = playlist_identity(player_state);
            if last_playlist_identity.as_ref() != Some(&current_identity) {
                playlist_version = playlist_version.wrapping_add(1).max(1);
                last_playlist_identity = Some(current_identity);
            }
            player_state.playlist_version = playlist_version;
        }
        if single_value(&shared_state) == SingleMode::OneShot
            && last_emitted_player_state.as_ref().map(track_identity)
                != player_state.as_ref().map(track_identity)
            && last_emitted_player_state.is_some()
            && player_state.is_some()
        {
            info!("Pausing WiiM after MPD single oneshot track change");
            if let Err(e) = wiim.send_player_cmd("pause").await {
                error!("Failed to execute single oneshot pause: {e}");
            }
            if let Err(e) = set_single_mode(&shared_state, SingleMode::Off) {
                error!("Failed to clear single oneshot state: {e}");
            }
        }
        try_set_player_state(
            &shared_state.player_state,
            player_state,
            &mut last_emitted_player_state,
        );

        let output_mode = match wiim.get_output_mode().await {
            Ok(output_mode) => {
                if output_mode_failures > 0 {
                    info!(
                        "Recovered reading WiiM output mode after {output_mode_failures} failed polls"
                    );
                    output_mode_failures = 0;
                }
                output_mode
            }
            Err(e) => {
                output_mode_failures += 1;
                if output_mode_failures == 1 {
                    warn!("Failed to read WiiM output mode, keeping last known output mode: {e}");
                } else if output_mode_failures == WIIM_READ_FAILURE_CLEAR_THRESHOLD {
                    warn!(
                        "Failed to read WiiM output mode {output_mode_failures} consecutive times, clearing output mode: {e}"
                    );
                } else {
                    debug!(
                        "Failed to read WiiM output mode ({output_mode_failures} consecutive failures): {e}"
                    );
                }
                if output_mode_failures >= WIIM_READ_FAILURE_CLEAR_THRESHOLD {
                    None
                } else {
                    last_emitted_output_mode
                }
            }
        };
        try_set_output_mode(
            &shared_state.output_mode,
            output_mode,
            &mut last_emitted_output_mode,
        );

        if preset_poll_ticks == 0 {
            let presets = match wiim.get_presets().await {
                Ok(presets) => {
                    if preset_failures > 0 {
                        info!(
                            "Recovered reading WiiM presets after {preset_failures} failed polls"
                        );
                        preset_failures = 0;
                    }
                    presets
                }
                Err(e) => {
                    preset_failures += 1;
                    if preset_failures == 1 {
                        warn!("Failed to read WiiM presets, keeping last known presets: {e}");
                    } else if preset_failures == WIIM_READ_FAILURE_CLEAR_THRESHOLD {
                        warn!(
                            "Failed to read WiiM presets {preset_failures} consecutive times, clearing presets: {e}"
                        );
                    } else {
                        debug!(
                            "Failed to read WiiM presets ({preset_failures} consecutive failures): {e}"
                        );
                    }
                    if preset_failures >= WIIM_READ_FAILURE_CLEAR_THRESHOLD {
                        Vec::new()
                    } else {
                        last_emitted_presets.clone()
                    }
                }
            };
            try_set_presets(&shared_state.presets, presets, &mut last_emitted_presets);
        }
        preset_poll_ticks = (preset_poll_ticks + 1) % 30;
        if last_emitted_player_state.is_none() {
            sleep(Duration::from_millis(1500)).await;
        }
    }
}

async fn handle_wiim_command(wiim: &WiimClient, command: Command, shared_state: &MpdSharedState) {
    debug!("Handle command {command:?}");
    let result = match command {
        Command::Play => wiim.send_player_cmd("resume").await,
        Command::Pause => wiim.send_player_cmd("pause").await,
        Command::Stop => wiim.send_player_cmd("stop").await,
        Command::Next => wiim.send_player_cmd("next").await,
        Command::Prev => wiim.send_player_cmd("prev").await,
        Command::Seek(seconds) => wiim.send_player_cmd(&format!("seek:{seconds}")).await,
        Command::SetVolume(volume) => wiim.send_player_cmd(&format!("vol:{volume}")).await,
        Command::ChangeVolume(change) => {
            let current_volume = shared_state
                .player_state
                .read()
                .ok()
                .and_then(|state| state.as_ref().and_then(|state| state.volume))
                .unwrap_or(0);
            let volume = (current_volume as i16 + change as i16).clamp(0, 100) as u8;
            wiim.send_player_cmd(&format!("vol:{volume}")).await
        }
        Command::SetRepeat(value) => {
            apply_loop_command(wiim, shared_state, Some(value), None).await
        }
        Command::SetRandom(value) => {
            apply_loop_command(wiim, shared_state, None, Some(value)).await
        }
        Command::SetSingle(value) => set_single_mode(shared_state, value),
        Command::SetOutput(output_mode) => wiim.set_output_mode(output_mode).await,
        Command::PlayPreset(number) => wiim
            .command_text(&format!("MCUKeyShortClick:{number}"))
            .await
            .map(|_| ()),
    };
    if let Err(e) = result {
        error!("Failed to execute WiiM command: {e}");
    }
}

async fn apply_loop_command(
    wiim: &WiimClient,
    shared_state: &MpdSharedState,
    repeat: Option<bool>,
    random: Option<bool>,
) -> anyhow::Result<()> {
    let mut loop_state = shared_state
        .player_state
        .read()
        .ok()
        .and_then(|state| state.as_ref().map(|state| state.loop_state.clone()))
        .unwrap_or_default();
    if let Some(repeat) = repeat {
        loop_state.repeat = repeat;
    }
    if let Some(random) = random {
        loop_state.random = random;
    }

    let command_value = match (loop_state.repeat, loop_state.random) {
        (true, true) => Some("2"),
        (true, false) => Some("-1"),
        (false, false) => Some("0"),
        (false, true) => None,
    };
    if let Some(value) = command_value {
        wiim.send_player_cmd(&format!("loopmode:{value}")).await?;
    } else {
        debug!("Accepting unsupported MPD loop combination as compatibility no-op: {loop_state:?}");
    }
    Ok(())
}

fn set_single_mode(shared_state: &MpdSharedState, single: SingleMode) -> anyhow::Result<()> {
    let mut options = shared_state
        .playback_options
        .write()
        .map_err(|_| anyhow::anyhow!("Failed to write playback options"))?;
    options.single = single;
    Ok(())
}

async fn read_wiim_state(wiim: &WiimClient) -> anyhow::Result<PlayerState> {
    let status = wiim.get_player_status().await?;
    let meta = match wiim.get_meta_info().await {
        Ok(meta) => meta,
        Err(e) => {
            warn!("Failed to read WiiM metadata: {e}");
            WiimMetaInfo::default()
        }
    };
    Ok(PlayerState {
        playback_status: parse_playback_status(status.status.as_deref()),
        title: meta.title,
        artist: meta.artist,
        album: meta.album,
        duration: parse_millis_as_secs(status.totlen.as_deref()).filter(|value| *value > 0.0),
        elapsed: parse_millis_as_secs(status.curpos.as_deref()),
        art_url: meta.art_url,
        volume: parse_u8(status.vol.as_deref()),
        mute: parse_bool_01(status.mute.as_deref()),
        loop_state: parse_loop_state(status.loop_mode.as_deref()),
        playlist_len: parse_u32(status.plicount.as_deref()),
        song_index: parse_u32(status.plicurr.as_deref()),
        playlist_version: 0,
    })
}

async fn handle_client(
    mut socket: TcpStream,
    command_tx: mpsc::Sender<Command>,
    shared_state: Arc<MpdSharedState>,
) {
    let mut buf = [0; 1024];
    if let Err(e) = socket.set_nodelay(true) {
        warn!("Failed to set nodelay: {e:?}");
    }
    if let Err(e) = socket.write_all(b"OK MPD 0.23.16\n").await {
        warn!("Failed to write greeting to socket: {e:?}");
        return;
    }

    let mut state = MpdQueryState {
        command_tx,
        pending_input: Vec::new(),
        in_command_list: false,
        in_command_list_ok: false,
        command_list_ended: false,
        command_list_count: 0,
        command_list_failed: false,
        last_idle_player_state: None,
        last_idle_playlist_state: None,
        last_idle_mixer_state: None,
        last_idle_output_state: None,
        should_close: false,
    };

    loop {
        let n = match socket.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(e) => {
                warn!("Failed to read from socket: {e:?}");
                return;
            }
        };
        if let Err(e) =
            handle_mpd_queries(&mut socket, &buf[0..n], &mut state, shared_state.clone()).await
        {
            warn!("Failed to handle MPD queries: {e:?}");
            return;
        }
        if state.should_close {
            return;
        }
    }
}

async fn handle_mpd_queries(
    socket: &mut TcpStream,
    commands: &[u8],
    state: &mut MpdQueryState,
    shared_state: Arc<MpdSharedState>,
) -> anyhow::Result<()> {
    let mut remainder = if state.pending_input.is_empty() {
        commands.to_vec()
    } else {
        let mut pending_input = std::mem::take(&mut state.pending_input);
        pending_input.extend_from_slice(commands);
        pending_input
    };
    loop {
        if remainder.is_empty() {
            break;
        }
        if remainder[0] == b'\n' || remainder[0] == b'\r' {
            remainder.remove(0);
            continue;
        }
        let new_remainder = match remainder.iter().position(|&b| b == b'\n' || b == b'\r') {
            Some(i) => remainder.split_off(i),
            None => {
                state.pending_input = remainder;
                break;
            }
        };
        if remainder.is_empty() {
            break;
        }
        match handle_mpd_query(&remainder, state, shared_state.clone(), socket).await {
            Ok(response) => {
                if !response.is_empty() {
                    socket.write_all(&response).await?;
                }
                if state.in_command_list_ok && !state.command_list_ended {
                    if state.command_list_count > 0 {
                        socket.write_all(b"list_OK\n").await?;
                    }
                } else if state.should_close {
                    return Ok(());
                } else {
                    socket.write_all(b"OK\n").await?;
                }
            }
            Err(e) => {
                state.command_list_failed = state.in_command_list;
                let error_response = format!(
                    "ACK [{}@{}] {} {}\n",
                    e.mpd_error_code, e.command_str, state.command_list_count, e
                );
                socket.write_all(error_response.as_bytes()).await?;
                break;
            }
        }
        remainder = new_remainder;
        if state.command_list_ended {
            state.in_command_list = false;
            state.in_command_list_ok = false;
            state.command_list_ended = false;
            state.command_list_failed = false;
            state.command_list_count = 0;
        } else if state.in_command_list {
            state.command_list_count += 1;
        }
    }
    Ok(())
}

async fn handle_mpd_query(
    command: &[u8],
    state: &mut MpdQueryState,
    shared_state: Arc<MpdSharedState>,
    socket: &mut TcpStream,
) -> Result<Vec<u8>, MpdCommandError> {
    let (command, arguments) = match command.iter().position(|&b| b == b' ') {
        Some(i) => (&command[0..i], &command[i + 1..command.len()]),
        None => (command, &[] as &[u8]),
    };
    if state.command_list_failed && command != b"command_list_end" {
        debug!(
            "Ignore list command while in failed state: {}",
            safe_command_print(command)
        );
        return Ok(Vec::new());
    }
    let result = match command {
        b"ping" => Ok(Vec::new()),
        b"binarylimit" => handle_binarylimit(arguments),
        b"commands" => handle_commands(),
        b"tagtypes" => handle_tagtypes(),
        b"play" => handle_play(state, shared_state.clone()).await,
        b"pause" => handle_pause(arguments, state).await,
        b"stop" => enqueue_command(state, Command::Stop).await,
        b"next" => enqueue_command(state, Command::Next).await,
        b"previous" => enqueue_command(state, Command::Prev).await,
        b"seek" => handle_seek(arguments, state).await,
        b"seekcur" => handle_seekcur(arguments, state, shared_state.clone()).await,
        b"repeat" => handle_bool_command(arguments, state, Command::SetRepeat).await,
        b"random" => handle_bool_command(arguments, state, Command::SetRandom).await,
        b"single" => handle_single(arguments, state).await,
        b"currentsong" => handle_current_song(shared_state),
        b"status" => handle_status(shared_state),
        b"outputs" => handle_outputs(shared_state),
        b"enableoutput" => handle_enable_output(arguments, state).await,
        b"disableoutput" => handle_disable_output(arguments).await,
        b"toggleoutput" => handle_toggle_output(arguments, state, shared_state.clone()).await,
        b"idle" => handle_idle(arguments, state, shared_state, socket).await,
        b"command_list_begin" => {
            state.in_command_list = true;
            Ok(Vec::new())
        }
        b"command_list_ok_begin" => {
            state.in_command_list = true;
            state.in_command_list_ok = true;
            Ok(Vec::new())
        }
        b"command_list_end" => {
            state.command_list_ended = true;
            Ok(Vec::new())
        }
        b"close" => {
            state.should_close = true;
            Ok(Vec::new())
        }
        b"volume" => handle_volume(arguments, state).await,
        b"setvol" => handle_setvol(arguments, state).await,
        b"getvol" => handle_getvol(shared_state),
        b"playlistinfo" => handle_playlist_info(shared_state),
        b"lsinfo" => handle_lsinfo(shared_state),
        b"add" => handle_add(arguments, shared_state),
        b"clear" => handle_clear(shared_state),
        b"stats" | b"noidle" => Ok(Vec::new()),
        _ => Err(anyhow::anyhow!(
            "Unknown command: {}",
            safe_command_print(command)
        )),
    };
    result.map_err(|e| MpdCommandError::new(command, &format!("{e:?}")))
}

fn handle_commands() -> anyhow::Result<Vec<u8>> {
    Ok("command: binarylimit\n\
         command: close\n\
         command: add\n\
         command: clear\n\
         command: commands\n\
         command: currentsong\n\
         command: disableoutput\n\
         command: enableoutput\n\
         command: getvol\n\
         command: idle\n\
         command: lsinfo\n\
         command: next\n\
         command: outputs\n\
         command: pause\n\
         command: ping\n\
         command: play\n\
         command: playlistinfo\n\
         command: previous\n\
         command: random\n\
         command: repeat\n\
         command: seek\n\
         command: seekcur\n\
         command: setvol\n\
         command: single\n\
         command: stats\n\
         command: status\n\
         command: stop\n\
         command: tagtypes\n\
         command: toggleoutput\n\
         command: volume\n"
        .into())
}

fn handle_binarylimit(arguments: &[u8]) -> anyhow::Result<Vec<u8>> {
    let _ = unquote_bytes(arguments)?.parse::<usize>()?;
    Ok(Vec::new())
}

fn handle_tagtypes() -> anyhow::Result<Vec<u8>> {
    Ok("tagtype: Artist\n\
        tagtype: Album\n\
        tagtype: Title\n"
        .into())
}

async fn enqueue_command(state: &mut MpdQueryState, command: Command) -> anyhow::Result<Vec<u8>> {
    state.command_tx.send(command).await?;
    Ok(Vec::new())
}

async fn handle_play(
    state: &mut MpdQueryState,
    shared_state: Arc<MpdSharedState>,
) -> anyhow::Result<Vec<u8>> {
    let selected_preset = shared_state
        .selected_preset
        .read()
        .ok()
        .and_then(|selected_preset| *selected_preset);
    if let Some(number) = selected_preset {
        enqueue_command(state, Command::PlayPreset(number)).await
    } else {
        enqueue_command(state, Command::Play).await
    }
}

async fn handle_pause(arguments: &[u8], state: &mut MpdQueryState) -> anyhow::Result<Vec<u8>> {
    match unquote_bytes(arguments)?.as_str() {
        "0" => enqueue_command(state, Command::Play).await,
        "" | "1" => enqueue_command(state, Command::Pause).await,
        _ => enqueue_command(state, Command::Play).await,
    }
}

async fn handle_bool_command(
    arguments: &[u8],
    state: &mut MpdQueryState,
    make_command: fn(bool) -> Command,
) -> anyhow::Result<Vec<u8>> {
    let argument = unquote_bytes(arguments)?;
    let value = match argument.as_str() {
        "0" => false,
        "1" | "oneshot" => true,
        _ => return Err(anyhow::anyhow!("Unsupported boolean argument {argument}")),
    };
    enqueue_command(state, make_command(value)).await
}

async fn handle_single(arguments: &[u8], state: &mut MpdQueryState) -> anyhow::Result<Vec<u8>> {
    let argument = unquote_bytes(arguments)?;
    let value = match argument.as_str() {
        "0" => SingleMode::Off,
        "1" => SingleMode::On,
        "oneshot" => SingleMode::OneShot,
        _ => return Err(anyhow::anyhow!("Unsupported single argument {argument}")),
    };
    enqueue_command(state, Command::SetSingle(value)).await
}

fn single_value(shared_state: &MpdSharedState) -> SingleMode {
    shared_state
        .playback_options
        .read()
        .ok()
        .map(|options| options.single)
        .unwrap_or_default()
}

fn single_mpd_value(single: SingleMode) -> &'static str {
    match single {
        SingleMode::Off => "0",
        SingleMode::On => "1",
        SingleMode::OneShot => "oneshot",
    }
}

async fn handle_seek(arguments: &[u8], state: &mut MpdQueryState) -> anyhow::Result<Vec<u8>> {
    let arguments = unquote_bytes(arguments)?;
    let seconds = arguments
        .split_whitespace()
        .nth(1)
        .or_else(|| arguments.split_whitespace().next())
        .context("Missing seek position")?
        .parse::<f32>()?
        .max(0.0) as u64;
    enqueue_command(state, Command::Seek(seconds)).await
}

async fn handle_seekcur(
    arguments: &[u8],
    state: &mut MpdQueryState,
    shared_state: Arc<MpdSharedState>,
) -> anyhow::Result<Vec<u8>> {
    let argument = unquote_bytes(arguments)?;
    let current = shared_state
        .player_state
        .read()
        .ok()
        .and_then(|state| state.as_ref().and_then(|state| state.elapsed))
        .unwrap_or(0.0);
    let seconds = if let Some(relative) = argument.strip_prefix(['+', '-']) {
        let delta = relative.parse::<f32>()?;
        if argument.starts_with('-') {
            (current - delta).max(0.0)
        } else {
            current + delta
        }
    } else {
        argument.parse::<f32>()?.max(0.0)
    } as u64;
    enqueue_command(state, Command::Seek(seconds)).await
}

async fn handle_volume(arguments: &[u8], state: &mut MpdQueryState) -> anyhow::Result<Vec<u8>> {
    let change = unquote_bytes(arguments)?.parse::<i8>()?;
    enqueue_command(state, Command::ChangeVolume(change)).await
}

async fn handle_setvol(arguments: &[u8], state: &mut MpdQueryState) -> anyhow::Result<Vec<u8>> {
    let volume = unquote_bytes(arguments)?.parse::<u8>()?.min(100);
    enqueue_command(state, Command::SetVolume(volume)).await
}

fn handle_getvol(shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    let volume = shared_state
        .player_state
        .read()
        .ok()
        .and_then(|state| state.as_ref().and_then(|state| state.volume))
        .unwrap_or(0);
    Ok(format!("volume: {volume}\n").into())
}

fn handle_outputs(shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    let active_output = shared_state
        .output_mode
        .read()
        .ok()
        .and_then(|output_mode| *output_mode);
    let mut response = Vec::new();
    for output_mode in OUTPUT_MODES {
        response.extend_from_slice(
            format!(
                "outputid: {}\noutputname: {}\noutputenabled: {}\n",
                output_mode.id(),
                output_mode.name(),
                bool_num(active_output == Some(output_mode)),
            )
            .as_bytes(),
        );
    }
    Ok(response)
}

async fn handle_enable_output(
    arguments: &[u8],
    state: &mut MpdQueryState,
) -> anyhow::Result<Vec<u8>> {
    let output_mode = parse_output_argument(arguments)?;
    enqueue_command(state, Command::SetOutput(output_mode)).await
}

async fn handle_disable_output(arguments: &[u8]) -> anyhow::Result<Vec<u8>> {
    let _ = parse_output_argument(arguments)?;
    Ok(Vec::new())
}

async fn handle_toggle_output(
    arguments: &[u8],
    state: &mut MpdQueryState,
    shared_state: Arc<MpdSharedState>,
) -> anyhow::Result<Vec<u8>> {
    let output_mode = parse_output_argument(arguments)?;
    let active_output = shared_state
        .output_mode
        .read()
        .ok()
        .and_then(|output_mode| *output_mode);
    if active_output == Some(output_mode) {
        Ok(Vec::new())
    } else {
        enqueue_command(state, Command::SetOutput(output_mode)).await
    }
}

fn handle_current_song(shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    let Some(player_state) = shared_state
        .player_state
        .read()
        .ok()
        .and_then(|state| state.clone())
    else {
        return Ok(Vec::new());
    };
    let mut response = Vec::new();
    let file = player_state
        .title
        .as_deref()
        .or(player_state.artist.as_deref())
        .unwrap_or("wiim-current-track");
    response.extend_from_slice(format!("file: {file}\n").as_bytes());
    if let Some(title) = &player_state.title {
        response.extend_from_slice(format!("Title: {title}\n").as_bytes());
    }
    if let Some(artist) = &player_state.artist {
        response.extend_from_slice(format!("Artist: {artist}\n").as_bytes());
    }
    if let Some(album) = &player_state.album {
        response.extend_from_slice(format!("Album: {album}\n").as_bytes());
    }
    if let Some(duration) = player_state.duration {
        response.extend_from_slice(
            format!("Time: {duration:.0}\nduration: {duration:.3}\n").as_bytes(),
        );
    }
    if let Some(art_url) = &player_state.art_url {
        response.extend_from_slice(format!("arturl: {art_url}\n").as_bytes());
    }
    append_song_identity(&mut response, &player_state);
    Ok(response)
}

fn handle_playlist_info(shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    if let Some(preset) = selected_preset(&shared_state) {
        let mut response = Vec::new();
        append_preset_song(&mut response, &preset);
        return Ok(response);
    }
    handle_current_song(shared_state)
}

fn handle_lsinfo(shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    let presets = shared_state
        .presets
        .read()
        .map(|presets| presets.clone())
        .unwrap_or_default();
    let mut response = Vec::new();
    for preset in presets {
        response.extend_from_slice(format!("file: preset:{}\n", preset.number).as_bytes());
        response.extend_from_slice(format!("Title: {}\n", preset.name).as_bytes());
        if let Some(source) = &preset.source {
            response.extend_from_slice(format!("Artist: {source}\n").as_bytes());
        }
        if let Some(art_url) = &preset.art_url {
            response.extend_from_slice(format!("arturl: {art_url}\n").as_bytes());
        }
    }
    Ok(response)
}

fn handle_add(arguments: &[u8], shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    let argument = unquote_bytes(arguments)?;
    let Some(number) = argument.strip_prefix("preset:") else {
        return Err(anyhow::anyhow!("Only preset:N entries can be added"));
    };
    let number = number.parse::<u8>()?;
    let presets = shared_state
        .presets
        .read()
        .map_err(|_| anyhow::anyhow!("Failed to read presets"))?;
    if !presets.iter().any(|preset| preset.number == number) {
        return Err(anyhow::anyhow!("Unknown preset {number}"));
    }
    drop(presets);
    *shared_state
        .selected_preset
        .write()
        .map_err(|_| anyhow::anyhow!("Failed to write selected preset"))? = Some(number);
    Ok(Vec::new())
}

fn handle_clear(shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    *shared_state
        .selected_preset
        .write()
        .map_err(|_| anyhow::anyhow!("Failed to write selected preset"))? = None;
    Ok(Vec::new())
}

fn handle_status(shared_state: Arc<MpdSharedState>) -> anyhow::Result<Vec<u8>> {
    let Some(player_state) = shared_state
        .player_state
        .read()
        .ok()
        .and_then(|state| state.clone())
    else {
        return Ok(
            "repeat: 0\nrandom: 0\nsingle: 0\nplaylist: 0\nsong: 0\nsongid: 0\nplaylistlength: 0\nvolume: 0\nstate: stop\n"
                .into(),
        );
    };

    let state = match player_state.playback_status {
        PlaybackStatus::Play => "play",
        PlaybackStatus::Pause => "pause",
        PlaybackStatus::Stop | PlaybackStatus::Loading => "stop",
    };
    let loop_state = &player_state.loop_state;
    let single = single_mpd_value(single_value(&shared_state));
    let volume = player_state.volume.unwrap_or(0);
    let song = player_state.song_index.unwrap_or(0);
    let song_id = mpd_song_id(&player_state);
    let playlist_len = player_state.playlist_len.unwrap_or(1).max(1);
    let mut response = format!(
        "repeat: {}\nrandom: {}\nsingle: {}\nplaylist: {}\nsong: {song}\nsongid: {song_id}\nplaylistlength: {playlist_len}\nvolume: {volume}\nstate: {state}\n",
        bool_num(loop_state.repeat),
        bool_num(loop_state.random),
        single,
        player_state.playlist_version,
    )
    .into_bytes();
    if let Some(duration) = player_state.duration {
        response.extend_from_slice(format!("duration: {duration}\n").as_bytes());
    }
    if let Some(elapsed) = player_state.elapsed {
        response.extend_from_slice(format!("elapsed: {elapsed}\n").as_bytes());
        if let Some(duration) = player_state.duration {
            response.extend_from_slice(format!("time: {elapsed:.0}:{duration:.0}\n").as_bytes());
        }
    }
    if let Some(art_url) = &player_state.art_url {
        response.extend_from_slice(format!("arturl: {art_url}\n").as_bytes());
    }
    Ok(response)
}

async fn handle_idle(
    arguments: &[u8],
    state: &mut MpdQueryState,
    shared_state: Arc<MpdSharedState>,
    socket: &mut TcpStream,
) -> anyhow::Result<Vec<u8>> {
    let arguments = std::str::from_utf8(arguments)?;
    let idle_all = arguments.is_empty();
    let idle_player = idle_all || arguments.contains("player");
    let idle_playlist = idle_all || arguments.contains("playlist");
    let idle_mixer = idle_all || arguments.contains("mixer");
    let idle_output = idle_all || arguments.contains("output");
    if !idle_player && !idle_playlist && !idle_mixer && !idle_output {
        return Err(anyhow::anyhow!("No supported subsystem in {arguments}"));
    }

    loop {
        let current_raw_state = shared_state
            .player_state
            .read()
            .ok()
            .and_then(|state| state.clone());
        let single = single_value(&shared_state);
        if idle_player {
            let current_state = current_raw_state
                .as_ref()
                .map(|player_state| get_state_for_idle_player(player_state, single));
            if current_state != state.last_idle_player_state {
                state.last_idle_player_state = current_state;
                return Ok(b"changed: player\n".to_vec());
            }
        }
        if idle_playlist {
            let selected_preset = selected_preset_number(&shared_state);
            let current_state = current_raw_state
                .as_ref()
                .map(|player_state| get_state_for_idle_playlist(player_state, selected_preset));
            if current_state != state.last_idle_playlist_state {
                state.last_idle_playlist_state = current_state;
                return Ok(b"changed: playlist\n".to_vec());
            }
        }
        if idle_mixer {
            let current_state = current_raw_state
                .as_ref()
                .map(|state| (state.volume, state.mute));
            if current_state != state.last_idle_mixer_state {
                state.last_idle_mixer_state = current_state;
                return Ok(b"changed: mixer\n".to_vec());
            }
        }
        if idle_output {
            let current_state = shared_state
                .output_mode
                .read()
                .ok()
                .and_then(|output_mode| *output_mode);
            if current_state != state.last_idle_output_state {
                state.last_idle_output_state = current_state;
                return Ok(b"changed: output\n".to_vec());
            }
        }

        let mut buf = [0; 1024];
        match timeout(Duration::from_millis(333), socket.read(&mut buf)).await {
            Ok(Ok(0)) => {
                state.should_close = true;
                return Ok(Vec::new());
            }
            Ok(Ok(n)) => {
                if let Some(i) = buf[0..n].iter().position(|&b| b == b'\n' || b == b'\r') {
                    if &buf[0..i] == b"noidle" {
                        return Ok(Vec::new());
                    }
                }
            }
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {}
        }
    }
}

fn try_set_player_state(
    player_state: &Arc<RwLock<Option<PlayerState>>>,
    value: Option<PlayerState>,
    last_emitted_value: &mut Option<PlayerState>,
) {
    if *last_emitted_value == value {
        return;
    }
    match player_state.write() {
        Ok(mut guard) => {
            *guard = value.clone();
            *last_emitted_value = value;
            trace!("Player state updated");
        }
        Err(_) => error!("Failed to write player state"),
    }
}

fn try_set_output_mode(
    output_mode: &Arc<RwLock<Option<OutputMode>>>,
    value: Option<OutputMode>,
    last_emitted_value: &mut Option<OutputMode>,
) {
    if *last_emitted_value == value {
        return;
    }
    match output_mode.write() {
        Ok(mut guard) => {
            *guard = value;
            *last_emitted_value = value;
            trace!("Output mode updated");
        }
        Err(_) => error!("Failed to write output mode"),
    }
}

fn try_set_presets(
    presets: &Arc<RwLock<Vec<Preset>>>,
    value: Vec<Preset>,
    last_emitted_value: &mut Vec<Preset>,
) {
    if *last_emitted_value == value {
        return;
    }
    match presets.write() {
        Ok(mut guard) => {
            *guard = value.clone();
            *last_emitted_value = value;
            trace!("Presets updated");
        }
        Err(_) => error!("Failed to write presets"),
    }
}

fn selected_preset(shared_state: &MpdSharedState) -> Option<Preset> {
    let number = shared_state
        .selected_preset
        .read()
        .ok()
        .and_then(|selected_preset| *selected_preset)?;
    shared_state.presets.read().ok().and_then(|presets| {
        presets
            .iter()
            .find(|preset| preset.number == number)
            .cloned()
    })
}

fn selected_preset_number(shared_state: &MpdSharedState) -> Option<u8> {
    shared_state
        .selected_preset
        .read()
        .ok()
        .and_then(|selected_preset| *selected_preset)
}

fn append_preset_song(response: &mut Vec<u8>, preset: &Preset) {
    response.extend_from_slice(format!("file: preset:{}\n", preset.number).as_bytes());
    response.extend_from_slice(format!("Title: {}\n", preset.name).as_bytes());
    if let Some(source) = &preset.source {
        response.extend_from_slice(format!("Artist: {source}\n").as_bytes());
    }
    if let Some(url) = &preset.url {
        response.extend_from_slice(format!("X-WiiM-URL: {url}\n").as_bytes());
    }
    if let Some(art_url) = &preset.art_url {
        response.extend_from_slice(format!("arturl: {art_url}\n").as_bytes());
    }
    response.extend_from_slice(b"Pos: 0\nId: 0\n");
}

fn append_song_identity(response: &mut Vec<u8>, player_state: &PlayerState) {
    let pos = player_state.song_index.unwrap_or(0);
    let id = mpd_song_id(player_state);
    response.extend_from_slice(format!("Pos: {pos}\nId: {id}\n").as_bytes());
}

fn mpd_song_id(player_state: &PlayerState) -> u32 {
    player_state.playlist_version.max(1)
}

fn get_state_for_idle_player(player_state: &PlayerState, single: SingleMode) -> PlayerStateForIdle {
    PlayerStateForIdle {
        playback_status: player_state.playback_status,
        title: player_state.title.clone(),
        artist: player_state.artist.clone(),
        album: player_state.album.clone(),
        art_url: player_state.art_url.clone(),
        loop_state: player_state.loop_state.clone(),
        single,
        selected_preset: None,
    }
}

fn get_state_for_idle_playlist(
    player_state: &PlayerState,
    selected_preset: Option<u8>,
) -> PlayerStateForIdle {
    PlayerStateForIdle {
        playback_status: PlaybackStatus::Pause,
        title: player_state.title.clone(),
        artist: player_state.artist.clone(),
        album: player_state.album.clone(),
        art_url: None,
        loop_state: LoopState::default(),
        single: SingleMode::Off,
        selected_preset,
    }
}

fn track_identity(player_state: &PlayerState) -> (Option<String>, Option<String>, Option<String>) {
    (
        player_state.title.clone(),
        player_state.artist.clone(),
        player_state.album.clone(),
    )
}

fn playlist_identity(
    player_state: &PlayerState,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<u32>,
    Option<u32>,
    Option<u32>,
) {
    (
        player_state.title.clone(),
        player_state.artist.clone(),
        player_state.album.clone(),
        player_state.art_url.clone(),
        player_state
            .duration
            .map(|duration| (duration * 1000.0).round() as u32),
        player_state.playlist_len,
        player_state.song_index,
    )
}

fn parse_playback_status(value: Option<&str>) -> PlaybackStatus {
    match value {
        Some("play") => PlaybackStatus::Play,
        Some("pause") => PlaybackStatus::Pause,
        Some("loading") => PlaybackStatus::Loading,
        _ => PlaybackStatus::Stop,
    }
}

fn parse_loop_state(value: Option<&str>) -> LoopState {
    match value {
        Some("0") => LoopState {
            repeat: true,
            random: false,
        },
        Some("1") => LoopState {
            repeat: true,
            random: false,
        },
        Some("2") => LoopState {
            repeat: true,
            random: true,
        },
        Some("3") => LoopState {
            repeat: false,
            random: true,
        },
        _ => LoopState::default(),
    }
}

fn parse_output_argument(arguments: &[u8]) -> anyhow::Result<OutputMode> {
    let argument = unquote_bytes(arguments)?;
    parse_output_mode(&argument).with_context(|| format!("Unsupported output id {argument}"))
}

fn parse_output_mode(value: &str) -> Option<OutputMode> {
    match value {
        "1" => Some(OutputMode::Spdif),
        "2" => Some(OutputMode::Aux),
        "3" => Some(OutputMode::Coax),
        _ => None,
    }
}

fn parse_millis_as_secs(value: Option<&str>) -> Option<f32> {
    value?.parse::<f32>().ok().map(|value| value / 1000.0)
}

fn parse_u8(value: Option<&str>) -> Option<u8> {
    value?.parse::<u8>().ok()
}

fn parse_u32(value: Option<&str>) -> Option<u32> {
    value?.parse::<u32>().ok()
}

fn parse_bool_01(value: Option<&str>) -> Option<bool> {
    match value? {
        "0" => Some(false),
        "1" => Some(true),
        _ => None,
    }
}

fn json_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn unquote_bytes(bytes: &[u8]) -> anyhow::Result<String> {
    Ok(std::str::from_utf8(bytes)?.trim().replace('"', ""))
}

fn safe_command_print(command: &[u8]) -> &str {
    std::str::from_utf8(command).unwrap_or("[un-utf8 command]")
}

fn bool_num(value: bool) -> u8 {
    if value { 1 } else { 0 }
}
