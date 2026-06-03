package com.projecttower.sse;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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

    private final long handle;
    private final String url;
    private final Map<String, String> headers;
    private final boolean reconnect;
    private final ScheduledExecutorService scheduler;

    private volatile boolean stopped;
    private volatile Call call;
    private volatile int retryMs;
    private volatile String lastEventId;

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
        stopped = true;
        Call active = call;
        if (active != null) {
            active.cancel();
        }
        scheduler.shutdownNow();
    }

    private void start() {
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

        if (lastEventId != null && lastEventId.length() > 0) {
            builder.header("Last-Event-ID", lastEventId);
        }

        call = HTTP.newCall(builder.build());
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                if (stopped) {
                    return;
                }
                nativeOnError(handle, e.getMessage() == null ? "SSE request failed" : e.getMessage(), 0, reconnect, retryMs);
                scheduleReconnectOrClose();
            }

            @Override
            public void onResponse(Call call, Response response) {
                try {
                    if (!response.isSuccessful()) {
                        nativeOnError(handle, "SSE request failed with HTTP status " + response.code(), response.code(), reconnect, retryMs);
                        scheduleReconnectOrClose();
                        return;
                    }

                    nativeOnOpen(handle, response.code());
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
                } finally {
                    response.close();
                }
            }
        });
    }

    private void readStream(BufferedSource source) throws IOException {
        SseParser parser = new SseParser();
        while (!stopped) {
            String line = source.readUtf8Line();
            if (line == null) {
                break;
            }
            parser.processLine(line);
        }
    }

    private void scheduleReconnectOrClose() {
        if (stopped) {
            return;
        }

        if (!reconnect) {
            nativeOnClosed(handle);
            return;
        }

        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                start();
            }
        }, retryMs, TimeUnit.MILLISECONDS);
    }

    private final class SseParser {
        private final StringBuilder data = new StringBuilder();
        private String eventName = "";
        private String eventId = "";
        private boolean hasId = false;

        void processLine(String line) {
            if (line.endsWith("\r")) {
                line = line.substring(0, line.length() - 1);
            }

            if (line.length() == 0) {
                dispatch();
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
                data.append(value).append('\n');
            } else if ("event".equals(field)) {
                eventName = value;
            } else if ("id".equals(field)) {
                eventId = value;
                hasId = true;
                lastEventId = value;
                nativeOnLastEventId(handle, value);
            } else if ("retry".equals(field) && isInteger(value)) {
                retryMs = Integer.parseInt(value);
                nativeOnRetry(handle, retryMs);
            }
        }

        private void dispatch() {
            if (data.length() == 0) {
                reset();
                return;
            }

            data.setLength(data.length() - 1);
            nativeOnMessage(handle, eventName.length() == 0 ? "message" : eventName, data.toString(), hasId ? eventId : "");
            reset();
        }

        private void reset() {
            data.setLength(0);
            eventName = "";
            eventId = "";
            hasId = false;
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
    private static native void nativeOnMessage(long handle, String event, String data, String id);
    private static native void nativeOnError(long handle, String error, int status, boolean reconnect, int retryMs);
    private static native void nativeOnClosed(long handle);
    private static native void nativeOnRetry(long handle, int retryMs);
    private static native void nativeOnLastEventId(long handle, String id);
}

