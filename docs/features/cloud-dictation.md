# Cloud dictation

Labs › Vercel live transcription (`labs.vercel_transcription`, off by default)
replaces dictation's speech recognition with OpenAI's `gpt-live-transcribe`
through Vercel AI Gateway. This sends dictation audio off the computer; it needs
internet, gateway model access, and a configured gateway key. The existing local
model remains installed and selected for other capabilities and when the lab is
off. Meetings, Annotate word timestamps, file transcription, and host audio keep
their existing local engines. This feature does not bypass onboarding's local
model requirement.

```ts
Enable Labs › Vercel live transcription                 // dictate.gateway
├── Hold the dictation shortcut -> intentional capture
│   └── 16 kHz capture -> bounded worker -> 24 kHz PCM -> Vercel WebSocket
│       └── GPT Live Transcribe deltas -> Labs › Live text, if on
├── Release -> exact final clip, including the normal release tail
│   ├── Stream heard past that clip -> cancel it, replay only the final clip
│   └── Final transcript -> existing corrections, processing, paste, and History
├── Programmatic recording -> submit its final clip through the same gateway
├── Cancel/short tap/chord discard -> cancel the worker; no output
└── Missing key / network failure / full queue / provider error / timeout
    └── Fail with a bounded explanation; no local fallback and no partial paste
```

The key is read from `AI_GATEWAY_API_KEY`, or, for Finder/login launches that
do not inherit shell variables, `<Application Support>/hex/gateway-api-key`.
The latter is a plain UTF-8 key file separate from `settings.json`; on Unix it
must be a regular file owned by the current user with mode `0600`. Provision it
without putting its contents in commands, logs, version control, or screenshots.
On Windows use the per-user Application Support ACL. The gateway owns the
provider credentials; Hex only needs the gateway credential. Restart Hex after
changing its environment. The file is read per connection.

The native transport uses platform TLS verification and Hex's proxy selection.
HTTP CONNECT proxies are supported; HTTPS/SOCKS proxies report an unsupported
transport instead of being bypassed. Incoming frames are capped at 256 KiB and
transcript text at 1 MiB; at most four connection workers may remain active.
The audio queue holds about ten seconds and fails instead of blocking capture
or silently dropping words. Finalization waits at most twenty seconds. Gateway
session limits still apply; reconnection does not resume a recording. No
focused-window text or screen vocabulary is sent to this provider. An enabled
Screen vocabulary lab does not throw away this live stream to run Parakeet.

Sources: `src/transcription/gateway.rs`, `src/net/websocket.rs`,
`src/transcription/engine.rs`, `src/parakeet.rs`, `src/labs.rs`.

Checks: `transcription::gateway::tests` covers chunk-invariant resampling,
replacement of deltas by final text, and sanitized provider failures. The
ignored `public_fixture_streams_through_gateway` sends a public Free Spoken
Digit Dataset WAV through the native Rust transport with explicit credentials.
A passing gateway fixture is a transport check, not a recognition-quality
benchmark or proof of a physical shortcut, microphone, or target paste.
