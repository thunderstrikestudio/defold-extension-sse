# Defold SSE Extension

Standalone Defold native extension for Server-Sent Events (SSE) with custom request headers and a single Lua API across platforms.

## Installation

After publishing this folder as a public GitHub repository, create a release and add the release zip to your Defold project's `game.project` dependencies:

```text
https://github.com/<owner>/<repo>/archive/refs/tags/<version>.zip
```

Using a fixed release tag is recommended over `main.zip` so game builds stay reproducible.

## API

```lua
local handle, err = sse.connect(url, {
    headers = {
        authorization = "Bearer ...",
        ["Api-Version"] = "123",
        ["Realtime-Capabilities"] = "ack,resume",
    },
    last_event_id = "optional",
    reconnect = true,
    retry_ms = 3000,
}, function(self, event)
    if event.type == sse.EVENT_OPEN then
        print("SSE open", event.status)
    elseif event.type == sse.EVENT_MESSAGE then
        print(event.event, event.id, event.data)
    elseif event.type == sse.EVENT_ERROR then
        print("SSE error", event.status, event.error)
    elseif event.type == sse.EVENT_CLOSED then
        print("SSE closed")
    end
end)

if not handle then
    print("SSE failed: " .. tostring(err))
end
```

### Functions

- `sse.connect(url, options, callback)` opens an SSE stream and returns a numeric handle, or `nil, error`.
- `sse.disconnect(handle)` closes a stream. Calling it more than once is safe.
- `sse.is_supported()` returns whether this target has an adapter enabled.
- `sse.is_connected(handle)` returns whether the handle is currently open.
- `sse.set_debug(enabled)` enables lightweight native log output.

`connect` also accepts `sse.connect(url, callback)` when no options are needed.

### Constants

- `sse.EVENT_OPEN`
- `sse.EVENT_MESSAGE`
- `sse.EVENT_ERROR`
- `sse.EVENT_CLOSED`

### Event Table

All callbacks receive `function(self, event)`.

- Common fields: `event.type`, `event.handle`
- Open: `event.status`
- Message: `event.event`, `event.data`, `event.id`
- Error: `event.error`, `event.status`, `event.reconnect`, `event.retry_ms`
- Closed: no extra fields

## Behavior

- Lua callbacks are only invoked from the Defold extension `Update` function.
- Platform/background callbacks push events into a capped thread-safe queue.
- The queue cap defaults to `1024` events and emits `EVENT_ERROR` if overflow drops events.
- `disconnect(handle)` is safe when called twice and when called from inside its own callback.
- `data:` lines are joined with `\n`.
- `event:` defaults to `"message"` when absent.
- `id:` updates the stored last event id.
- `retry:` updates the reconnect delay when it is a valid integer.
- Comment lines starting with `:` are ignored.
- Unknown SSE fields are ignored.
- UTF-8 payload bytes are preserved by platform parsers and passed to Lua strings.

## Reconnect

Reconnect is disabled unless `options.reconnect == true`.

When enabled, adapters reconnect after close or error unless the user called `disconnect(handle)`. The current `Last-Event-ID` is sent on reconnect, using either the initial `last_event_id` option or the most recent `id:` field received from the stream. Custom headers are applied to every request.

## Platform Status

| Platform | Status | Notes |
| --- | --- | --- |
| macOS | Implemented | C++ worker thread with libcurl and native SSE parser. |
| Linux | Implemented | C++ worker thread with libcurl and native SSE parser. |
| Windows | Compile-safe unsupported stub | Add packaged libcurl headers/libs and enable `SSE_USE_LIBCURL` in `ext.manifest` to support Windows. |
| Android | Implemented | Java adapter using OkHttp streaming response and Java SSE parser. |
| iOS | Implemented | Objective-C++ adapter using `NSURLSessionDataDelegate` streaming and byte-oriented SSE parser. |
| HTML5 | Implemented | JavaScript `fetch` + `ReadableStream`, chosen so custom headers such as `Authorization` work. |
| Other | Compile-safe unsupported stub | `sse.is_supported()` returns false. |

## Build Notes

Defold native extensions are built by the Defold build server or a local Extender server. This repository is structured as a Defold library project:

```text
sse/
  ext.manifest
  include/
  src/
  manifests/android/
examples/
game.project
```

The Android adapter declares OkHttp in `sse/manifests/android/build.gradle`. The iOS adapter uses only Foundation and does not require CocoaPods.

Desktop macOS/Linux builds expect libcurl to be available in the native-extension build environment. If your Extender image does not provide libcurl, add platform libraries under `sse/lib/<arch-platform>/` and update `sse/ext.manifest`.

## Example

See `examples/sse_example.script` for a minimal script component. The sample URL is intentionally a placeholder; replace it with your own SSE endpoint.

## License

MIT. See `LICENSE`.

