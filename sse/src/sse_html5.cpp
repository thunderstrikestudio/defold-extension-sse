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

    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnMessage(int handle, const char* event_name, const char* data, const char* id)
    {
        SSE_EnqueueMessage((int32_t)handle, event_name, data, id);
        if (id && id[0])
        {
            SSE_SetLastEventId((int32_t)handle, id);
        }
    }

    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnError(int handle, const char* error, int status, int reconnecting, int retry_ms)
    {
        SSE_EnqueueError((int32_t)handle, error, (int32_t)status, reconnecting != 0, (int32_t)retry_ms);
    }

    EMSCRIPTEN_KEEPALIVE void SSE_Html5_OnClosed(int handle)
    {
        SSE_EnqueueClosed((int32_t)handle);
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
                    Module._SSE_Html5_OnMessage(handle, ep, dp, ip);
                    _free(ip);
                    _free(dp);
                    _free(ep);
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
                processLine: function(client, line) {
                    if (line.endsWith("\r")) {
                        line = line.substring(0, line.length - 1);
                    }
                    if (line.length === 0) {
                        Module.SSEExt.dispatch(client);
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
                        client.data += value + "\n";
                    } else if (field === "event") {
                        client.eventName = value;
                    } else if (field === "id") {
                        client.eventId = value;
                        client.hasId = true;
                        client.lastEventId = value;
                        Module.SSEExt.callLastEventId(client.handle, value);
                    } else if (field === "retry" && /^[0-9]+$/.test(value)) {
                        client.retryMs = parseInt(value, 10);
                    }
                },
                dispatch: function(client) {
                    if (client.data.length === 0) {
                        client.eventName = "";
                        client.eventId = "";
                        client.hasId = false;
                        return;
                    }
                    var data = client.data.substring(0, client.data.length - 1);
                    Module.SSEExt.callMessage(client.handle, client.eventName || "message", data, client.hasId ? client.eventId : "");
                    client.data = "";
                    client.eventName = "";
                    client.eventId = "";
                    client.hasId = false;
                },
                scheduleReconnectOrClose: function(client) {
                    if (client.stopped) {
                        return;
                    }
                    if (!client.reconnect) {
                        Module._SSE_Html5_OnClosed(client.handle);
                        return;
                    }
                    setTimeout(function() {
                        Module.SSEExt.connect(client);
                    }, client.retryMs);
                },
                connect: async function(client) {
                    if (client.stopped) {
                        return;
                    }

                    client.controller = new AbortController();
                    client.data = "";
                    client.eventName = "";
                    client.eventId = "";
                    client.hasId = false;

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

                    try {
                        if (typeof fetch !== "function" || typeof ReadableStream === "undefined") {
                            Module.SSEExt.callError(client.handle, "fetch ReadableStream is not available", 0, false, client.retryMs);
                            Module._SSE_Html5_OnClosed(client.handle);
                            return;
                        }

                        var response = await fetch(client.url, {
                            method: "GET",
                            headers: requestHeaders,
                            cache: "no-store",
                            signal: client.controller.signal
                        });

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
                        var reader = response.body.getReader();
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
                                Module.SSEExt.processLine(client, lines[i]);
                            }
                        }

                        pending += decoder.decode();
                        if (pending.length > 0) {
                            Module.SSEExt.processLine(client, pending);
                        }

                        if (!client.stopped) {
                            Module.SSEExt.scheduleReconnectOrClose(client);
                        }
                    } catch (error) {
                        if (client.stopped) {
                            return;
                        }
                        Module.SSEExt.callError(client.handle, error && error.message ? error.message : "SSE fetch failed", 0, client.reconnect, client.retryMs);
                        Module.SSEExt.scheduleReconnectOrClose(client);
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
            hasId: false
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

