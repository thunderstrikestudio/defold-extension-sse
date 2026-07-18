package com.projecttower.sse;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;

public final class SseClient {
    private static final OkHttpClient HTTP = new OkHttpClient.Builder()
            .connectTimeout(15, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .retryOnConnectionFailure(true)
            .build();

    // Reconnect backoff: the retry base is clamped to a sane range and grows
    // exponentially (capped) with +-20% jitter so clients do not stampede.
    private static final long RETRY_MIN_MS = 1000;
    private static final long RETRY_MAX_MS = 300000;
    private static final long MIN_DELAY_MS = 500;
    private static final int MAX_BACKOFF_SHIFT = 8;
    // Bound on the time between starting a request and receiving response
    // headers; an established stream may idle indefinitely.
    private static final long FIRST_RESPONSE_TIMEOUT_MS = 30000;
    // Caps on the pending line and accumulated event data buffers.
    private static final long MAX_LINE_BYTES = 1024 * 1024;
    private static final int MAX_DATA_CHARS = 1024 * 1024;

    private final long handle;
    private final String url;
    private final Map<String, String> headers;
    private final boolean reconnect;
    private final ScheduledExecutorService scheduler;
    private final Object lock = new Object();
    private final Random random = new Random();

    private volatile boolean stopped;
    private volatile Call call;
    private volatile int retryMs;
    private volatile String lastEventId;
    private int failedAttempts; // guarded by lock
    private ScheduledFuture<?> firstResponseWatchdog; // guarded by lock
    private boolean firstResponseTimedOut; // guarded by lock

    public SseClient(long handle, String url, String[] headerNames, String[] headerValues, String lastEventId, boolean reconnect, int retryMs) {
        this.handle = handle;
        this.url = url;
        this.lastEventId = lastEventId == null ? "" : lastEventId;
        this.reconnect = reconnect;
        this.retryMs = retryMs >= 0 ? retryMs : 3000;
        this.headers = new LinkedHashMap<String, String>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        if (headerNames != null && headerValues != null) {
            int count = Math.min(headerNames.length, headerValues.length);
            for (int i = 0; i < count; i++) {
                if (headerNames[i] != null && headerValues[i] != null) {
                    this.headers.put(headerNames[i], headerValues[i]);
                }
            }
        }
    }

    public void connect() {
        stopped = false;
        start();
    }

    public void disconnect() {
        // Synchronized with scheduleReconnectOrClose/start so no thread can
        // pass the stopped check and then schedule on a shut-down executor.
        synchronized (lock) {
            stopped = true;
            cancelFirstResponseWatchdogLocked();
            Call active = call;
            if (active != null) {
                active.cancel();
            }
            scheduler.shutdownNow();
        }
    }

    private void start() {
        final Call started;
        synchronized (lock) {
            if (stopped) {
                return;
            }

            Request.Builder builder = new Request.Builder()
                    .url(url)
                    .header("Accept", "text/event-stream")
                    .header("Cache-Control", "no-cache");

            for (Map.Entry<String, String> entry : headers.entrySet()) {
                builder.header(entry.getKey(), entry.getValue());
            }

            String resumeId = lastEventId;
            if (resumeId != null && resumeId.length() > 0) {
                builder.header("Last-Event-ID", resumeId);
            }

            started = HTTP.newCall(builder.build());
            call = started;
            firstResponseTimedOut = false;
            try {
                firstResponseWatchdog = scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        onFirstResponseTimeout(started);
                    }
                }, FIRST_RESPONSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // disconnect() raced with this attempt; nothing left to do.
                return;
            }
        }

        started.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                boolean timedOut = clearFirstResponseState();
                if (stopped) {
                    return;
                }
                if (!timedOut) {
                    nativeOnError(handle, e.getMessage() == null ? "SSE request failed" : e.getMessage(), 0, reconnect, retryMs);
                }
                scheduleReconnectOrClose();
            }

            @Override
            public void onResponse(Call call, Response response) {
                boolean timedOut = clearFirstResponseState();
                try {
                    if (timedOut) {
                        // The watchdog already reported this attempt as timed
                        // out and cancelled the call; don't surface an open
                        // (or messages) for it, just run the reconnect logic.
                        if (!stopped) {
                            scheduleReconnectOrClose();
                        }
                        return;
                    }

                    if (!response.isSuccessful()) {
                        nativeOnError(handle, "SSE request failed with HTTP status " + response.code(), response.code(), reconnect, retryMs);
                        scheduleReconnectOrClose();
                        return;
                    }

                    nativeOnOpen(handle, response.code());
                    synchronized (lock) {
                        failedAttempts = 0;
                    }

                    ResponseBody body = response.body();
                    if (body == null) {
                        nativeOnError(handle, "SSE response body was empty", response.code(), reconnect, retryMs);
                        scheduleReconnectOrClose();
                        return;
                    }

                    readStream(body.source());
                    if (!stopped) {
                        scheduleReconnectOrClose();
                    }
                } catch (IOException e) {
                    if (!stopped) {
                        nativeOnError(handle, e.getMessage() == null ? "SSE stream failed" : e.getMessage(), response.code(), reconnect, retryMs);
                        scheduleReconnectOrClose();
                    }
                } catch (Throwable t) {
                    // Never let a Throwable escape into OkHttp's dispatcher:
                    // an uncaught exception there kills the whole process.
                    if (!stopped) {
                        String message = t.getMessage();
                        nativeOnError(handle, message == null || message.length() == 0 ? "SSE stream failed: " + t.getClass().getName() : message, response.code(), reconnect, retryMs);
                        scheduleReconnectOrClose();
                    }
                } finally {
                    response.close();
                }
            }
        });
    }

    private void onFirstResponseTimeout(Call watched) {
        synchronized (lock) {
            if (stopped || call != watched || firstResponseWatchdog == null) {
                return;
            }
            firstResponseWatchdog = null;
            firstResponseTimedOut = true;
        }
        nativeOnError(handle, "SSE timed out waiting for server response", 0, reconnect, retryMs);
        watched.cancel();
    }

    private boolean clearFirstResponseState() {
        synchronized (lock) {
            cancelFirstResponseWatchdogLocked();
            boolean timedOut = firstResponseTimedOut;
            firstResponseTimedOut = false;
            return timedOut;
        }
    }

    private void cancelFirstResponseWatchdogLocked() {
        if (firstResponseWatchdog != null) {
            firstResponseWatchdog.cancel(false);
            firstResponseWatchdog = null;
        }
    }

    private void readStream(BufferedSource source) throws IOException {
        SseParser parser = new SseParser();
        while (!stopped) {
            long newline = source.indexOf((byte) '\n', 0, MAX_LINE_BYTES);
            if (newline != -1) {
                String line = source.readUtf8(newline);
                source.skip(1);
                parser.processLine(line);
                continue;
            }

            if (source.getBuffer().size() >= MAX_LINE_BYTES) {
                parser.poison("SSE line exceeded maximum buffer size");
                if (!skipUntilNewline(source)) {
                    break;
                }
                continue;
            }

            if (!source.request(source.getBuffer().size() + 1)) {
                break; // end of stream
            }
        }
    }

    private boolean skipUntilNewline(BufferedSource source) throws IOException {
        while (!stopped) {
            long newline = source.indexOf((byte) '\n', 0, MAX_LINE_BYTES);
            if (newline != -1) {
                source.skip(newline + 1);
                return true;
            }
            // No newline within the search window, so the first
            // min(buffered, window) bytes are all part of the oversized
            // line. Skip at most the window so bytes that okio may have
            // buffered beyond it (the newline and any following events)
            // are preserved for the next iteration.
            long buffered = source.getBuffer().size();
            if (buffered > 0) {
                source.skip(Math.min(buffered, MAX_LINE_BYTES));
            }
            if (!source.request(1)) {
                return false;
            }
        }
        return false;
    }

    private void scheduleReconnectOrClose() {
        // Reached whenever a stream attempt has ended (error, EOF, timeout),
        // so this is the one place the connected flag is truthfully cleared;
        // recoverable parser errors on a live stream do not go through here.
        nativeOnDisconnected(handle);

        synchronized (lock) {
            if (stopped) {
                return;
            }

            if (!reconnect) {
                nativeOnClosed(handle);
                return;
            }

            failedAttempts++;
            long delay = computeRetryDelayMsLocked();
            try {
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            start();
                        } catch (Throwable t) {
                            // e.g. Request.Builder.url() on a malformed URL;
                            // surface it as an error and keep the retry loop
                            // alive instead of silently dying.
                            if (!stopped) {
                                String message = t.getMessage();
                                nativeOnError(handle, message == null ? "SSE reconnect failed" : message, 0, reconnect, retryMs);
                                scheduleReconnectOrClose();
                            }
                        }
                    }
                }, delay, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // disconnect() raced with the reconnect; the client is done.
            }
        }
    }

    private long computeRetryDelayMsLocked() {
        long base = retryMs;
        if (base < RETRY_MIN_MS) {
            base = RETRY_MIN_MS;
        }
        if (base > RETRY_MAX_MS) {
            base = RETRY_MAX_MS;
        }

        int shift = failedAttempts > 1 ? failedAttempts - 1 : 0;
        if (shift > MAX_BACKOFF_SHIFT) {
            shift = MAX_BACKOFF_SHIFT;
        }

        long delay = base << shift;
        if (delay > RETRY_MAX_MS) {
            delay = RETRY_MAX_MS;
        }

        // +-20% jitter in 0.1% steps.
        long jitter = random.nextInt(401) - 200;
        delay += delay * jitter / 1000;
        if (delay < MIN_DELAY_MS) {
            delay = MIN_DELAY_MS;
        }
        return delay;
    }

    private final class SseParser {
        private final StringBuilder data = new StringBuilder();
        private String eventName = "";
        private String eventId = "";
        private boolean hasId = false;
        private boolean firstLine = true;
        private boolean eventPoisoned = false;
        // Persistent last-event-id buffer: updated by every id: line (id-only
        // blocks and empty spec-legal resets included), committed as the
        // reconnect resume position when the next event is delivered. Never
        // cleared by reset() so an id-only checkpoint is not lost.
        private String pendingLastEventId = null;
        // Rollback point: the buffer state at the end of the last completed
        // block, restored when a block is poisoned so a dropped block's id is
        // never committed as the resume position.
        private String pendingAtBlockStart = null;
        // Set once an event is dropped (queue full). From then on the resume
        // position is frozen for the rest of this stream attempt, so a later
        // accepted id can never commit past the undelivered event; the next
        // reconnect then replays from before the drop.
        private boolean dropObserved = false;

        void processLine(String line) {
            if (firstLine) {
                firstLine = false;
                if (line.length() > 0 && line.charAt(0) == '\uFEFF') {
                    line = line.substring(1);
                }
            }

            if (line.endsWith("\r")) {
                line = line.substring(0, line.length() - 1);
            }

            if (line.length() == 0) {
                dispatch();
                return;
            }

            if (eventPoisoned) {
                // The current event lost data to a buffer cap; ignore its
                // remaining field lines until the blank line ends it.
                return;
            }

            if (line.charAt(0) == ':') {
                return;
            }

            int colon = line.indexOf(':');
            String field = colon == -1 ? line : line.substring(0, colon);
            String value = colon == -1 ? "" : line.substring(colon + 1);
            if (value.startsWith(" ")) {
                value = value.substring(1);
            }

            if ("data".equals(field)) {
                // + 1 accounts for the appended newline, so a flood of empty
                // data: lines still trips the cap.
                if (data.length() + value.length() + 1 > MAX_DATA_CHARS) {
                    poison("SSE event data exceeded maximum buffer size");
                    return;
                }
                data.append(value).append('\n');
            } else if ("event".equals(field)) {
                eventName = value;
            } else if ("id".equals(field)) {
                eventId = value;
                hasId = true;
                pendingLastEventId = value;
            } else if ("retry".equals(field) && isInteger(value)) {
                long parsed;
                try {
                    parsed = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    // Digit-only value beyond Long range: treat as very
                    // large so it clamps to the maximum like the other
                    // platforms instead of being silently ignored.
                    parsed = Long.MAX_VALUE;
                }
                if (parsed < RETRY_MIN_MS) {
                    parsed = RETRY_MIN_MS;
                }
                if (parsed > RETRY_MAX_MS) {
                    parsed = RETRY_MAX_MS;
                }
                retryMs = (int) parsed;
                nativeOnRetry(handle, retryMs);
            }
        }

        void poison(String message) {
            nativeOnError(handle, message, 0, reconnect, retryMs);
            // Roll back any id: line the poisoned block may already have
            // parsed, so a dropped block can never advance the committed
            // resume position.
            pendingLastEventId = pendingAtBlockStart;
            reset();
            eventPoisoned = true;
        }

        private void dispatch() {
            if (eventPoisoned) {
                eventPoisoned = false;
                reset();
                return;
            }

            if (data.length() == 0) {
                if (hasId && !dropObserved && pendingLastEventId != null) {
                    // An id-only checkpoint block carries no payload that
                    // could be dropped; commit the resume position now so it
                    // survives a connection drop before the next data event.
                    lastEventId = pendingLastEventId;
                    nativeOnLastEventId(handle, pendingLastEventId);
                }
                reset();
                return;
            }

            data.setLength(data.length() - 1);
            boolean enqueued = nativeOnMessage(handle, eventName.length() == 0 ? "message" : eventName, data.toString(), hasId ? eventId : "");
            if (enqueued) {
                if (!dropObserved && pendingLastEventId != null) {
                    // Commit the persistent buffer only when the event was
                    // actually delivered; otherwise a reconnect would skip
                    // the events dropped on queue overflow. An empty value is
                    // a spec-legal reset and clears the Last-Event-ID header.
                    lastEventId = pendingLastEventId;
                    nativeOnLastEventId(handle, pendingLastEventId);
                }
            } else {
                // The event was dropped (queue full); roll back its id and
                // freeze the resume position for the rest of this attempt,
                // so neither this nor any later id can commit past an event
                // the callback never received. The next reconnect replays
                // from before the drop.
                pendingLastEventId = pendingAtBlockStart;
                dropObserved = true;
            }
            reset();
        }

        private void reset() {
            data.setLength(0);
            eventName = "";
            eventId = "";
            hasId = false;
            // The block is over; its id state becomes the rollback point for
            // a future poisoned block.
            pendingAtBlockStart = pendingLastEventId;
        }

        private boolean isInteger(String value) {
            if (value.length() == 0) {
                return false;
            }
            for (int i = 0; i < value.length(); i++) {
                if (!Character.isDigit(value.charAt(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    private static native void nativeOnOpen(long handle, int status);
    private static native boolean nativeOnMessage(long handle, String event, String data, String id);
    private static native void nativeOnError(long handle, String error, int status, boolean reconnect, int retryMs);
    private static native void nativeOnClosed(long handle);
    private static native void nativeOnRetry(long handle, int retryMs);
    private static native void nativeOnLastEventId(long handle, String id);
    private static native void nativeOnDisconnected(long handle);
}
