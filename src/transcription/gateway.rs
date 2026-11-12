//! Opt-in dictation through Vercel's streaming transcription protocol.
//! Audio leaves the computer only while the lab is enabled. This path never
//! falls back to local inference or sends focused-window text to the provider.
use super::engine::{Partial, Partials, Source, Transcribed};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{self, Receiver, SyncSender, TryRecvError},
};
use std::time::{Duration, Instant};
use tungstenite::Message;

const URL: &str =
    "wss://ai-gateway.vercel.sh/v4/ai/transcription-model?ai-model-id=openai%2Fgpt-live-transcribe";
const FINISH: Duration = Duration::from_secs(20);
const MAX_TEXT: usize = 1024 * 1024;
static ACTIVE: AtomicUsize = AtomicUsize::new(0);

enum Input {
    Audio(Vec<f32>),
    Finish,
}

pub(crate) struct Stream {
    boundary: AudioBoundary,
    input: SyncSender<Input>,
    result: Option<Receiver<Result<String, String>>>,
    cancel: Arc<AtomicBool>,
}

pub(crate) struct Pending {
    result: Receiver<Result<String, String>>,
    cancel: Arc<AtomicBool>,
    started: Instant,
}

impl Stream {
    /// Returns immediately; connecting and all network I/O run off capture.
    pub(crate) fn begin(partials: Option<Partials>) -> Self {
        Self::with_audio(partials, Vec::new())
    }

    pub(crate) fn with_audio(partials: Option<Partials>, initial: Vec<f32>) -> Self {
        let (input, audio) = mpsc::sync_channel(64);
        let (reply, result) = mpsc::sync_channel(1);
        let cancel = Arc::new(AtomicBool::new(false));
        let cancelled = cancel.clone();
        let spawn = std::thread::Builder::new()
            .name("gateway-transcription".into())
            .spawn(move || {
                let outcome = if ACTIVE.fetch_add(1, Ordering::AcqRel) >= 4 {
                    ACTIVE.fetch_sub(1, Ordering::AcqRel);
                    Err("Too many gateway dictations are still finishing".into())
                } else {
                    let result = run(audio, &cancelled, partials, initial);
                    ACTIVE.fetch_sub(1, Ordering::AcqRel);
                    result
                };
                let _ = reply.send(outcome);
            });
        if spawn.is_err() {
            cancel.store(true, Ordering::Release);
        }
        Self {
            boundary: AudioBoundary::default(),
            input,
            result: Some(result),
            cancel,
        }
    }

    pub(crate) fn feed(&mut self, samples: &[f32]) {
        let ready = self.boundary.feed(samples);
        self.send(&ready);
    }

    fn send(&self, samples: &[f32]) {
        // At most about ten seconds queue behind a slow network. A full
        // queue fails this capture instead of blocking or losing words.
        for chunk in samples.chunks(2560) {
            if self.input.try_send(Input::Audio(chunk.to_vec())).is_err() {
                self.cancel.store(true, Ordering::Release);
                break;
            }
        }
    }

    pub(crate) fn finish_clip(mut self, clip: &[f32]) -> Option<Pending> {
        self.boundary.pending = self.boundary.remaining(clip)?.to_vec();
        Some(self.finish())
    }

    pub(crate) fn finish(mut self) -> Pending {
        let tail = std::mem::take(&mut self.boundary.pending);
        self.send(&tail);
        if self.input.try_send(Input::Finish).is_err() {
            self.cancel.store(true, Ordering::Release);
        }
        Pending {
            result: self.result.take().unwrap(),
            cancel: self.cancel.clone(),
            started: Instant::now(),
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if self.result.is_some() {
            self.cancel.store(true, Ordering::Release);
        }
    }
}

impl Pending {
    pub(crate) fn wait(self) -> Result<Transcribed, String> {
        let text = self
            .result
            .recv_timeout(FINISH)
            .map_err(|_| "Vercel live transcription timed out or stopped".to_string())??;
        Ok(Transcribed {
            text,
            source: Source::Gateway,
            finish_ms: self.started.elapsed().as_millis() as u64,
        })
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
    }
}

fn key() -> Result<String, String> {
    if let Ok(key) = std::env::var("AI_GATEWAY_API_KEY")
        && !key.trim().is_empty()
    {
        return Ok(key.trim().to_owned());
    }
    // Finder/login launches don't inherit a shell's environment. The owner's
    // key is provisioned separately from settings and is never serialized.
    let path = crate::app_paths::support_dir()
        .map_err(|_| "Couldn't locate the gateway key")?
        .join("gateway-api-key");
    let metadata = std::fs::symlink_metadata(&path)
        .map_err(|_| "Configure an AI Gateway key before using Vercel live transcription")?;
    if !metadata.is_file() || metadata.len() > 4096 {
        return Err("Invalid gateway key file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
            return Err(
                "The gateway key file must be owned by you and readable only by you".into(),
            );
        }
    }
    let key = std::fs::read_to_string(path).map_err(|_| "Couldn't read the gateway key")?;
    let key = key.trim();
    if key.is_empty() {
        return Err("The gateway key is empty".into());
    }
    Ok(key.to_owned())
}

fn run(
    audio: Receiver<Input>,
    cancel: &AtomicBool,
    partials: Option<Partials>,
    initial: Vec<f32>,
) -> Result<String, String> {
    let key = key()?;
    if cancel.load(Ordering::Acquire) {
        return Err("Gateway dictation cancelled".into());
    }
    let mut socket = crate::net::websocket::connect(URL, &key)?;
    socket.send(Message::text(json!({"type":"transcription-stream.start", "inputAudioFormat":{"type":"audio/pcm","rate":24000}}).to_string()))
        .map_err(|_| "Couldn't start Vercel live transcription")?;
    let mut resampler = Pcm::default();
    let mut transcript = Transcript::default();
    let mut finishing: Option<Instant> = None;
    let mut offset = 0;
    let outcome = (|| {
        loop {
            if cancel.load(Ordering::Acquire) {
                return Err("Gateway dictation cancelled or its audio queue filled".into());
            }
            if finishing.is_some_and(|at| at.elapsed() >= FINISH) {
                return Err("Vercel live transcription timed out".into());
            }
            if finishing.is_none() {
                // Keep reads interleaved with writes so partials and errors
                // arrive even during a long dictation.
                for _ in 0..4 {
                    if offset < initial.len() {
                        let end = (offset + 2560).min(initial.len());
                        socket
                            .send(Message::binary(resampler.encode(&initial[offset..end])))
                            .map_err(|_| "Couldn't send audio to Vercel")?;
                        offset = end;
                        continue;
                    }
                    match audio.try_recv() {
                        Ok(Input::Audio(samples)) => socket
                            .send(Message::binary(resampler.encode(&samples)))
                            .map_err(|_| "Couldn't send audio to Vercel")?,
                        Ok(Input::Finish) => {
                            socket
                                .send(Message::text(
                                    r#"{"type":"transcription-stream.audio-done"}"#,
                                ))
                                .map_err(|_| "Couldn't finish Vercel live transcription")?;
                            finishing = Some(Instant::now());
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            return Err("Gateway dictation cancelled".into());
                        }
                    }
                }
            }
            match socket.read() {
                Ok(Message::Text(text)) => {
                    let event: Value = serde_json::from_str(&text)
                        .map_err(|_| "Invalid gateway transcription response")?;
                    if transcript.event(&event)? {
                        if finishing.is_none() {
                            return Err(
                                "Vercel ended the transcription before the recording finished"
                                    .into(),
                            );
                        }
                        return Ok(transcript.text().trim().to_owned());
                    }
                    if let Some(partials) = &partials {
                        partials.publish(Partial {
                            stable: transcript.committed.clone(),
                            tentative: transcript.tentative.clone(),
                        });
                    }
                }
                Ok(Message::Close(_)) => {
                    return Err("Vercel closed the transcription before it finished".into());
                }
                Ok(_) => {}
                Err(tungstenite::Error::Io(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => return Err("The Vercel transcription connection failed".into()),
            }
        }
    })();
    let _ = socket.close(None);
    outcome
}

/// Gateway finals replace a turn's partials; they must never be appended
/// beside the same text's deltas. Finished turns form the committed prefix.
#[derive(Default)]
struct Transcript {
    committed: String,
    tentative: String,
    finalized: bool,
}
impl Transcript {
    fn text(&self) -> String {
        format!("{}{}", self.committed, self.tentative)
    }
    fn event(&mut self, event: &Value) -> Result<bool, String> {
        match event["type"].as_str() {
            Some("transcript-delta") => {
                self.tentative.push_str(event["delta"].as_str().ok_or("Invalid gateway transcript delta")?);
            }
            Some("transcript-final") => {
                let text = event["text"].as_str().ok_or("Invalid gateway final transcript")?;
                if !self.committed.is_empty() && !text.starts_with(char::is_whitespace) { self.committed.push(' '); }
                self.committed.push_str(text);
                self.tentative.clear();
                self.finalized = true;
            }
            Some("error") => return Err("Vercel couldn't transcribe this dictation; check gateway model access, billing, and provider keys".into()),
            Some("finish") => {
                if !self.finalized { return Err("Vercel returned no final transcript".into()); }
                self.tentative.clear();
                return Ok(true);
            }
            _ => {}
        }
        if self.committed.len() + self.tentative.len() > MAX_TEXT {
            return Err("The gateway transcript exceeded its size limit".into());
        }
        Ok(false)
    }
}

/// Projects capture feeds onto the exact final clip without replaying audio.
#[derive(Default)]
struct AudioBoundary {
    pending: Vec<f32>,
    sent: usize,
}
impl AudioBoundary {
    fn feed(&mut self, samples: &[f32]) -> Vec<f32> {
        // Match the engine's 250 ms release-trimming allowance. Keep that
        // small tail unsent so a slightly delayed release can trim it without
        // opening another connection and retranscribing the entire recording.
        const TAIL: usize = 16_000 / 4;
        self.pending.extend_from_slice(samples);
        let ready = self.pending.len().saturating_sub(TAIL);
        self.sent += ready;
        self.pending.drain(..ready).collect()
    }
    fn remaining<'a>(&self, clip: &'a [f32]) -> Option<&'a [f32]> {
        clip.get(self.sent..)
    }
}

/// Stateful 16 -> 24 kHz linear interpolation, independent of feed boundaries.
#[derive(Default)]
struct Pcm {
    previous: f32,
    samples: u64,
    next: u64,
}
impl Pcm {
    fn encode(&mut self, samples: &[f32]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(samples.len() * 3 + 2);
        for &sample in samples {
            let sample = if sample.is_finite() {
                sample.clamp(-1.0, 1.0)
            } else {
                0.0
            };
            let position = self.samples * 3;
            while self.next <= position {
                let value = if self.samples == 0 {
                    sample
                } else {
                    let fraction = (self.next - (position - 3)) as f32 / 3.0;
                    self.previous + (sample - self.previous) * fraction
                };
                bytes.extend_from_slice(&((value * 32767.0).round() as i16).to_le_bytes());
                self.next += 2;
            }
            self.previous = sample;
            self.samples += 1;
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_slightly_late_release_keeps_the_existing_gateway_connection() {
        let captured: Vec<f32> = (0..9600).map(|i| i as f32).collect();
        let final_clip = &captured[..9300];
        let mut boundary = AudioBoundary::default();
        let mut sent = boundary.feed(&captured);
        let tail = boundary
            .remaining(final_clip)
            .expect("ordinary release trimming must not reopen and retranscribe the whole clip");
        sent.extend_from_slice(tail);
        assert_eq!(sent, final_clip);
    }

    #[test]
    fn a_release_before_already_sent_audio_still_requires_replay() {
        let mut boundary = AudioBoundary::default();
        boundary.feed(&[0.0; 16000]);
        assert!(boundary.remaining(&[0.0; 8000]).is_none());
    }

    #[test]
    fn resampling_is_independent_of_chunk_boundaries() {
        let samples: Vec<f32> = (0..16000).map(|i| (i as f32 * 0.01).sin()).collect();
        let whole = Pcm::default().encode(&samples);
        let mut pcm = Pcm::default();
        let chunks: Vec<u8> = samples
            .chunks(137)
            .flat_map(|chunk| pcm.encode(chunk))
            .collect();
        assert_eq!(whole, chunks);
        assert_eq!(whole.len(), 47998);
    }
    #[test]
    fn finals_replace_deltas_and_finish_requires_a_final() {
        let mut text = Transcript::default();
        text.event(&json!({"type":"transcript-delta","delta":"helo"}))
            .unwrap();
        text.event(&json!({"type":"transcript-final","text":"Hello."}))
            .unwrap();
        assert_eq!(text.text(), "Hello.");
        assert!(text.event(&json!({"type":"finish"})).unwrap());
        assert!(
            Transcript::default()
                .event(&json!({"type":"finish"}))
                .is_err()
        );
    }
    #[test]
    fn multiple_finals_keep_each_turn_once_and_silence_can_finish() {
        let mut text = Transcript::default();
        text.event(&json!({"type":"transcript-final","text":"First."}))
            .unwrap();
        text.event(&json!({"type":"transcript-delta","delta":"secon"}))
            .unwrap();
        text.event(&json!({"type":"transcript-final","text":"Second."}))
            .unwrap();
        assert!(text.event(&json!({"type":"finish"})).unwrap());
        assert_eq!(text.text(), "First. Second.");
        let mut silence = Transcript::default();
        silence
            .event(&json!({"type":"transcript-final","text":""}))
            .unwrap();
        assert!(silence.event(&json!({"type":"finish"})).unwrap());
        assert!(silence.text().is_empty());
    }

    #[test]
    fn a_full_audio_queue_cancels_instead_of_dropping_words() {
        let (input, _audio) = mpsc::sync_channel(1);
        let (_reply, result) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut stream = Stream {
            boundary: AudioBoundary::default(),
            input,
            result: Some(result),
            cancel: cancel.clone(),
        };
        stream.feed(&[0.0; 6560]);
        assert!(!cancel.load(Ordering::Acquire));
        stream.feed(&[0.0; 2560]);
        assert!(cancel.load(Ordering::Acquire));
    }

    #[test]
    fn dropping_a_recording_cancels_but_finishing_keeps_it_until_the_reply() {
        let (input, audio) = mpsc::sync_channel(1);
        let (_reply, result) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let stream = Stream {
            boundary: AudioBoundary::default(),
            input,
            result: Some(result),
            cancel: cancel.clone(),
        };
        let pending = stream.finish();
        assert!(matches!(audio.try_recv(), Ok(Input::Finish)));
        assert!(!cancel.load(Ordering::Acquire));
        drop(pending);
        assert!(cancel.load(Ordering::Acquire));
        let (input, _audio) = mpsc::sync_channel(1);
        let (_reply, result) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        drop(Stream {
            boundary: AudioBoundary::default(),
            input,
            result: Some(result),
            cancel: cancel.clone(),
        });
        assert!(cancel.load(Ordering::Acquire));
    }

    #[test]
    fn provider_errors_do_not_expose_server_content() {
        let error = Transcript::default()
            .event(&json!({"type":"error","error":"private provider content"}))
            .unwrap_err();
        assert!(!error.contains("private provider content"));
    }
    #[test]
    #[ignore = "times a public JFK fixture through Vercel; requires AI_GATEWAY_API_KEY and HEX_GATEWAY_LATENCY_WAV"]
    fn public_late_release_does_not_retranscribe_the_recording() {
        let path = std::env::var("HEX_GATEWAY_LATENCY_WAV").expect("public JFK WAV path");
        let mut wav = hound::WavReader::open(path).unwrap();
        assert_eq!(wav.spec().sample_rate, 16000);
        assert_eq!(wav.spec().channels, 1);
        let samples: Vec<f32> = wav
            .samples::<i16>()
            .map(|s| s.unwrap() as f32 / 32768.0)
            .collect();
        let mut stream = Stream::begin(None);
        for chunk in samples.chunks(1600) {
            stream.feed(chunk);
            std::thread::sleep(Duration::from_millis(100));
        }
        let released = Instant::now();
        // The audio callback ran 100 ms past the authoritative release.
        // This must keep the already-open connection, not upload it again.
        let pending = stream
            .finish_clip(&samples[..samples.len() - 1600])
            .expect("ordinary release trimming must keep the live connection");
        let text = pending.wait().unwrap().text.to_lowercase();
        assert!(text.contains("country"));
        println!(
            "Public late-release fixture: final text in {} ms",
            released.elapsed().as_millis()
        );
        assert!(
            released.elapsed() < Duration::from_secs(2),
            "release-to-text exceeded two seconds"
        );
    }

    #[test]
    #[ignore = "sends a public fixture to the live Vercel gateway; requires AI_GATEWAY_API_KEY"]
    fn public_fixture_streams_through_gateway() {
        let path = std::env::var("HEX_GATEWAY_TEST_WAV").expect("public WAV fixture path");
        let mut wav = hound::WavReader::open(path).unwrap();
        assert_eq!(wav.spec().sample_rate, 16000);
        assert_eq!(wav.spec().channels, 1);
        let samples: Vec<f32> = wav
            .samples::<i16>()
            .map(|s| s.unwrap() as f32 / 32768.0)
            .collect();
        let partials = Partials::default();
        let mut stream = Stream::begin(Some(partials));
        for chunk in samples.chunks(1600) {
            stream.feed(chunk);
            std::thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(stream.finish().wait().unwrap().text.to_lowercase(), "zero.");
    }
}
