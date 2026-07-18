#if defined(DM_PLATFORM_HTML5)

#include "sse_private.h"

#include <emscripten/emscripten.h>
#include <string>

static void SSEHTML5_AppendEscaped(std::string& out, const char* value)
{
    out += '"';
    if (value)
    {
        for (const char* c = value; *c; ++c)
        {
            switch (*c)
            {
                case '\\': out += "\\\\"; break;
                case '"': out += "\\\""; break;
                case '\b': out += "\\b"; break;
                case '\f': out += "\\f"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                default:
                    if ((unsigned char)*c < 0x20)
                    {
                        char buffer[8];
                        dmSnPrintf(buffer, sizeof(buffer), "\\u%04x", (unsigned char)*c);
                        out += buffer;
                    }
                    else
                    {
                        out += *c;
                    }
                    break;
            }
        }
    }
    out += '"';
}

static std::string SSEHTML5_BuildHeadersJson(SSEConnection* connection)
{
    std::string json = "{";
    for (uint32_t i = 0; i < connection->m_Headers.Size(); ++i)
    {
        if (i > 0)
        {
            json += ",";
        }
        SSEHTML5_AppendEscaped(json, connection->m_Headers[i].m_Name);
        json += ":";
        SSEHTML5_AppendEscaped(json, connection->m_Headers[i].m_Value);
    }
    json += "}";
    return json;
}

extern "C"
{
    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnOpen(int handle, int status)
    {
        SSE_EnqueueOpen((int32_t)handle, (int32_t)status);
    }

    EMSCRIPTEN_KEEPALIVE int SSE_Html5_OnMessage(int handle, const char* event_name, const char* data, const char* id)
    {
        // Returns whether the event was accepted by the queue; the JS side
        // commits the resume position (via SSE_Html5_OnLastEventId) only for
        // delivered events, so a reconnect does not skip dropped events.
        return SSE_EnqueueMessage((int32_t)handle, event_name, data, id) ? 1 : 0;
    }

    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnError(int handle, const char* error, int status, int reconnecting, int retry_ms)
    {
        SSE_EnqueueError((int32_t)handle, error, (int32_t)status, reconnecting != 0, (int32_t)retry_ms);
    }

    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnClosed(int handle)
    {
        SSE_EnqueueClosed((int32_t)handle);
    }

    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnDisconnected(int handle)
    {
        // Called when a stream attempt ends (error, EOF, timeout) so
        // sse.is_connected() stays truthful during outages. Recoverable
        // parser errors on a live stream deliberately do not reach this.
        SSE_SetConnected((int32_t)handle, false);
    }

    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnLastEventId(int handle, const char* id)
    {
        SSE_SetLastEventId((int32_t)handle, id);
    }
}

bool SSE_Platform_Initialize()
{
    EM_ASM({
        if (!Module.SSEExt) {
            Module.SSEExt = {
                clients: {},
                // Cap on the pending line and accumulated event data buffers.
                maxBuffer: 1024 * 1024,
                // Reconnect backoff clamps (1s..5min) and time-to-first-response bound.
                retryMinMs: 1000,
                retryMaxMs: 300000,
                minDelayMs: 500,
                maxBackoffShift: 8,
                firstResponseTimeoutMs: 30000,
                allocString: function(value) {
                    value = value || "";
                    var length = lengthBytesUTF8(value) + 1;
                    var ptr = _malloc(length);
                    stringToUTF8(value, ptr, length);
                    return ptr;
                },
                callMessage: function(handle, eventName, data, id) {
                    var ep = Module.SSEExt.allocString(eventName);
                    var dp = Module.SSEExt.allocString(data);
                    var ip = Module.SSEExt.allocString(id);
                    var enqueued = Module._SSE_Html5_OnMessage(handle, ep, dp, ip);
                    _free(ip);
                    _free(dp);
                    _free(ep);
                    return enqueued !== 0;
                },
                callError: function(handle, error, status, reconnecting, retryMs) {
                    var ep = Module.SSEExt.allocString(error);
                    Module._SSE_Html5_OnError(handle, ep, status || 0, reconnecting ? 1 : 0, retryMs || 0);
                    _free(ep);
                },
                callLastEventId: function(handle, id) {
                    var ip = Module.SSEExt.allocString(id);
                    Module._SSE_Html5_OnLastEventId(handle, ip);
                    _free(ip);
                },
                resetEvent: function(client) {
                    client.data = "";
                    client.eventName = "";
                    client.eventId = "";
                    client.hasId = false;
                    // The block is over; its id state becomes the rollback
                    // point for a future poisoned block.
                    client.pendingAtBlockStart = client.pendingLastEventId;
                },
                poison: function(client, message) {
                    // The current event lost data to a buffer cap; report it
                    // and suppress the event until its terminating blank line
                    // so a truncated payload is never delivered as complete.
                    // Any id: the poisoned block already parsed is rolled
                    // back so a dropped block can never advance the committed
                    // resume position.
                    Module.SSEExt.callError(client.handle, message, 0, client.reconnect, client.retryMs);
                    client.pendingLastEventId = client.pendingAtBlockStart;
                    Module.SSEExt.resetEvent(client);
                    client.eventPoisoned = true;
                },
                processLine: function(client, line) {
                    if (line.endsWith("\r")) {
                        line = line.substring(0, line.length - 1);
                    }
                    if (line.length === 0) {
                        Module.SSEExt.dispatch(client);
                        return;
                    }
                    if (client.eventPoisoned) {
                        return;
                    }
                    if (line.charAt(0) === ":") {
                        return;
                    }
                    var colon = line.indexOf(":");
                    var field = colon === -1 ? line : line.substring(0, colon);
                    var value = colon === -1 ? "" : line.substring(colon + 1);
                    if (value.charAt(0) === " ") {
                        value = value.substring(1);
                    }
                    if (field === "data") {
                        if (client.data.length + value.length > Module.SSEExt.maxBuffer) {
                            Module.SSEExt.poison(client, "SSE event data exceeded maximum buffer size");
                            return;
                        }
                        client.data += value + "\n";
                    } else if (field === "event") {
                        client.eventName = value;
                    } else if (field === "id") {
                        client.eventId = value;
                        client.hasId = true;
                        // Persistent buffer: survives id-only blocks and
                        // carries empty spec-legal resets; committed when the
                        // next event is delivered.
                        client.pendingLastEventId = value;
                    } else if (field === "retry" && /^[0-9]+$/.test(value)) {
                        var parsed = parseInt(value, 10);
                        if (parsed < Module.SSEExt.retryMinMs) {
                            parsed = Module.SSEExt.retryMinMs;
                        }
                        if (parsed > Module.SSEExt.retryMaxMs) {
                            parsed = Module.SSEExt.retryMaxMs;
                        }
                        client.retryMs = parsed;
                    }
                },
                dispatch: function(client) {
                    if (client.eventPoisoned) {
                        client.eventPoisoned = false;
                        Module.SSEExt.resetEvent(client);
                        return;
                    }
                    if (client.data.length === 0) {
                        if (client.hasId && client.pendingLastEventId !== null) {
                            // An id-only checkpoint block carries no payload
                            // that could be dropped; commit the resume
                            // position now so it survives a connection drop
                            // before the next data event.
                            client.lastEventId = client.pendingLastEventId;
                            Module.SSEExt.callLastEventId(client.handle, client.pendingLastEventId);
                        }
                        Module.SSEExt.resetEvent(client);
                        return;
                    }
                    var data = client.data.substring(0, client.data.length - 1);
                    var enqueued = Module.SSEExt.callMessage(client.handle, client.eventName || "message", data, client.hasId ? client.eventId : "");
                    if (enqueued && client.pendingLastEventId !== null) {
                        // Commit the persistent buffer only when the event
                        // was actually delivered; otherwise a reconnect
                        // would skip the events dropped on queue overflow.
                        client.lastEventId = client.pendingLastEventId;
                        Module.SSEExt.callLastEventId(client.handle, client.pendingLastEventId);
                    }
                    Module.SSEExt.resetEvent(client);
                },
                computeRetryDelay: function(client) {
                    var base = client.retryMs;
                    if (!(base >= Module.SSEExt.retryMinMs)) {
                        base = Module.SSEExt.retryMinMs;
                    }
                    if (base > Module.SSEExt.retryMaxMs) {
                        base = Module.SSEExt.retryMaxMs;
                    }
                    var shift = client.failedAttempts > 1 ? client.failedAttempts - 1 : 0;
                    if (shift > Module.SSEExt.maxBackoffShift) {
                        shift = Module.SSEExt.maxBackoffShift;
                    }
                    var delay = base * Math.pow(2, shift);
                    if (delay > Module.SSEExt.retryMaxMs) {
                        delay = Module.SSEExt.retryMaxMs;
                    }
                    // +-20% jitter so reconnecting clients spread out.
                    delay = delay * (0.8 + Math.random() * 0.4);
                    if (delay < Module.SSEExt.minDelayMs) {
                        delay = Module.SSEExt.minDelayMs;
                    }
                    return Math.floor(delay);
                },
                scheduleReconnectOrClose: function(client) {
                    // Reached whenever a stream attempt has ended, so this is
                    // the one place the connected flag is truthfully cleared;
                    // recoverable parser errors on a live stream do not come
                    // through here.
                    Module._SSE_Html5_OnDisconnected(client.handle);
                    if (client.stopped) {
                        return;
                    }
                    if (!client.reconnect) {
                        Module._SSE_Html5_OnClosed(client.handle);
                        return;
                    }
                    client.failedAttempts = (client.failedAttempts || 0) + 1;
                    setTimeout(function() {
                        Module.SSEExt.connect(client);
                    }, Module.SSEExt.computeRetryDelay(client));
                },
                connect: async function(client) {
                    if (client.stopped) {
                        return;
                    }

                    // Feature-check before touching any of the APIs so an
                    // unsupported runtime fails with a visible error instead
                    // of a rejected promise nobody observes.
                    if (typeof fetch !== "function" || typeof ReadableStream === "undefined" || typeof AbortController === "undefined") {
                        Module.SSEExt.callError(client.handle, "fetch streaming (fetch/ReadableStream/AbortController) is not available", 0, false, client.retryMs);
                        Module._SSE_Html5_OnClosed(client.handle);
                        return;
                    }

                    client.controller = new AbortController();
                    Module.SSEExt.resetEvent(client);
                    client.pendingLastEventId = null;
                    client.pendingAtBlockStart = null;
                    client.eventPoisoned = false;
                    client.discardLine = false;
                    client.gotResponse = false;
                    client.firstResponseTimedOut = false;

                    var requestHeaders = {
                        "Accept": "text/event-stream",
                        "Cache-Control": "no-cache"
                    };
                    Object.keys(client.headers).forEach(function(key) {
                        requestHeaders[key] = client.headers[key];
                    });
                    if (client.lastEventId) {
                        requestHeaders["Last-Event-ID"] = client.lastEventId;
                    }

                    // Bound time-to-first-response only; an opened stream may
                    // idle indefinitely.
                    var watchdog = setTimeout(function() {
                        if (!client.stopped && !client.gotResponse) {
                            client.firstResponseTimedOut = true;
                            if (client.controller) {
                                client.controller.abort();
                            }
                        }
                    }, Module.SSEExt.firstResponseTimeoutMs);

                    try {
                        var response = await fetch(client.url, {
                            method: "GET",
                            headers: requestHeaders,
                            cache: "no-store",
                            signal: client.controller.signal
                        });

                        client.gotResponse = true;

                        if (!response.ok) {
                            Module.SSEExt.callError(client.handle, "SSE request failed with HTTP status " + response.status, response.status, client.reconnect, client.retryMs);
                            Module.SSEExt.scheduleReconnectOrClose(client);
                            return;
                        }

                        if (!response.body || !response.body.getReader) {
                            Module.SSEExt.callError(client.handle, "ReadableStream body is not available", response.status, client.reconnect, client.retryMs);
                            Module.SSEExt.scheduleReconnectOrClose(client);
                            return;
                        }

                        Module._SSE_Html5_OnOpen(client.handle, response.status);
                        client.failedAttempts = 0;
                        var reader = response.body.getReader();
                        // TextDecoder (ignoreBOM defaults to false) strips a
                        // leading UTF-8 BOM from the stream by itself.
                        var decoder = new TextDecoder("utf-8");
                        var pending = "";

                        while (!client.stopped) {
                            var result = await reader.read();
                            if (result.done) {
                                break;
                            }
                            pending += decoder.decode(result.value, { stream: true });
                            var lines = pending.split("\n");
                            pending = lines.pop();
                            for (var i = 0; i < lines.length; i++) {
                                if (client.discardLine) {
                                    // Tail of an oversized line; drop it.
                                    client.discardLine = false;
                                    continue;
                                }
                                if (lines[i].length > Module.SSEExt.maxBuffer) {
                                    // A complete oversized line inside one
                                    // chunk must not bypass the cap.
                                    Module.SSEExt.poison(client, "SSE line exceeded maximum buffer size");
                                    continue;
                                }
                                Module.SSEExt.processLine(client, lines[i]);
                            }
                            if (pending.length > Module.SSEExt.maxBuffer) {
                                if (!client.discardLine) {
                                    Module.SSEExt.poison(client, "SSE line exceeded maximum buffer size");
                                    client.discardLine = true;
                                }
                                pending = "";
                            }
                        }

                        pending += decoder.decode();
                        if (pending.length > 0 && !client.discardLine) {
                            if (pending.length > Module.SSEExt.maxBuffer) {
                                Module.SSEExt.poison(client, "SSE line exceeded maximum buffer size");
                            } else {
                                Module.SSEExt.processLine(client, pending);
                            }
                        }

                        if (!client.stopped) {
                            Module.SSEExt.scheduleReconnectOrClose(client);
                        }
                    } catch (error) {
                        if (client.stopped) {
                            return;
                        }
                        if (client.firstResponseTimedOut) {
                            Module.SSEExt.callError(client.handle, "SSE timed out waiting for server response", 0, client.reconnect, client.retryMs);
                        } else {
                            Module.SSEExt.callError(client.handle, error && error.message ? error.message : "SSE fetch failed", 0, client.reconnect, client.retryMs);
                        }
                        Module.SSEExt.scheduleReconnectOrClose(client);
                    } finally {
                        clearTimeout(watchdog);
                    }
                }
            };
        }
    });
    return true;
}

void SSE_Platform_Finalize()
{
    EM_ASM({
        if (Module.SSEExt && Module.SSEExt.clients) {
            Object.keys(Module.SSEExt.clients).forEach(function(handle) {
                var client = Module.SSEExt.clients[handle];
                client.stopped = true;
                if (client.controller) {
                    client.controller.abort();
                }
            });
            Module.SSEExt.clients = {};
        }
    });
}

bool SSE_Platform_IsSupported()
{
    return true;
}

bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size)
{
    std::string headers = SSEHTML5_BuildHeadersJson(connection);
    const char* last_event_id = connection->m_LastEventId ? connection->m_LastEventId : "";

    EM_ASM({
        var handle = $0;
        var url = UTF8ToString($1);
        var headers = JSON.parse(UTF8ToString($2) || "{}");
        var lastEventId = UTF8ToString($3);
        var reconnect = $4 !== 0;
        var retryMs = $5;
        var client = {
            handle: handle,
            url: url,
            headers: headers,
            lastEventId: lastEventId,
            reconnect: reconnect,
            retryMs: retryMs,
            stopped: false,
            controller: null,
            data: "",
            eventName: "",
            eventId: "",
            hasId: false,
            pendingLastEventId: null,
            pendingAtBlockStart: null,
            eventPoisoned: false,
            discardLine: false,
            gotResponse: false,
            firstResponseTimedOut: false,
            failedAttempts: 0
        };
        Module.SSEExt.clients[handle] = client;
        Module.SSEExt.connect(client);
    }, connection->m_Handle, connection->m_Url, headers.c_str(), last_event_id, connection->m_Reconnect ? 1 : 0, connection->m_RetryMS);

    connection->m_PlatformData = (void*)1;
    (void)error;
    (void)error_size;
    return true;
}

void SSE_Platform_Disconnect(SSEConnection* connection)
{
    EM_ASM({
        var handle = $0;
        if (Module.SSEExt && Module.SSEExt.clients && Module.SSEExt.clients[handle]) {
            var client = Module.SSEExt.clients[handle];
            client.stopped = true;
            if (client.controller) {
                client.controller.abort();
            }
            delete Module.SSEExt.clients[handle];
        }
    }, connection->m_Handle);
    connection->m_PlatformData = 0;
}

bool SSE_Platform_IsConnected(SSEConnection* connection)
{
    return connection->m_Connected != 0;
}

void SSE_Platform_Update()
{
}

#endif

