#ifndef EXTENSION_SSE_CONNECTION_POOL_IMPL_H
#define EXTENSION_SSE_CONNECTION_POOL_IMPL_H

#include <dmsdk/dlib/connection_pool.h>
#include <dmsdk/dlib/socket.h>
#include <dmsdk/dlib/sslsocket.h>

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>

#define SSE_DM_CONNECT_TIMEOUT_US (15000000)
#define SSE_DM_SOCKET_WAIT_US (100000)

struct SSEDesktopConnection
{
    int32_t m_Handle;
    char* m_Url;
    char* m_LastEventId;
    dmArray<SSEHeader> m_Headers;
    SSEParser m_Parser;
    dmThread::Thread m_Thread;
    dmMutex::HMutex m_Mutex;
    dmConnectionPool::HConnection m_Connection;
    dmSocket::Socket m_Socket;
    dmSSLSocket::Socket m_SSLSocket;
    int32_t m_RetryMS;
    int32_t m_Status;
    int m_CancelFlag;
    uint8_t m_Reconnect;
    uint8_t m_Stop;
    uint8_t m_Opened;
    uint8_t m_Secure;

    SSEDesktopConnection()
    : m_Handle(0)
    , m_Url(0)
    , m_LastEventId(0)
    , m_Thread(0)
    , m_Mutex(0)
    , m_Connection(0)
    , m_Socket(0)
    , m_SSLSocket(0)
    , m_RetryMS(3000)
    , m_Status(0)
    , m_CancelFlag(0)
    , m_Reconnect(0)
    , m_Stop(0)
    , m_Opened(0)
    , m_Secure(0)
    {
    }
};

struct SSEDMUrl
{
    std::string m_Host;
    std::string m_Path;
    std::string m_HostHeader;
    int m_Port;
    uint8_t m_Secure;

    SSEDMUrl()
    : m_Port(0)
    , m_Secure(0)
    {
    }
};

struct SSEDMChunkDecoder
{
    enum State
    {
        STATE_SIZE,
        STATE_DATA,
        STATE_DATA_CRLF,
        STATE_DATA_LF,
        STATE_DONE
    };

    std::string m_Line;
    size_t m_Remaining;
    State m_State;

    SSEDMChunkDecoder()
    : m_Remaining(0)
    , m_State(STATE_SIZE)
    {
    }
};

static dmConnectionPool::HPool g_SSEConnectionPool = 0;

static char* SSEDesktop_StrDup(const char* value)
{
    if (!value)
    {
        return 0;
    }
    const size_t length = strlen(value);
    char* copy = (char*)malloc(length + 1);
    if (copy)
    {
        memcpy(copy, value, length + 1);
    }
    return copy;
}

static void SSEDesktop_SetString(char** target, const char* value)
{
    free(*target);
    *target = SSEDesktop_StrDup(value ? value : "");
}

static bool SSEDesktop_ShouldStop(SSEDesktopConnection* connection)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    return connection->m_Stop != 0;
}

static void SSEDM_SetConnection(SSEDesktopConnection* connection, dmConnectionPool::HConnection pool_connection)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    connection->m_Connection = pool_connection;
}

static dmConnectionPool::HConnection SSEDM_TakeConnection(SSEDesktopConnection* connection)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    dmConnectionPool::HConnection pool_connection = connection->m_Connection;
    connection->m_Connection = 0;
    connection->m_Socket = 0;
    connection->m_SSLSocket = 0;
    return pool_connection;
}

static void SSEDM_CloseActiveConnection(SSEDesktopConnection* connection)
{
    dmConnectionPool::HConnection pool_connection = SSEDM_TakeConnection(connection);
    if (pool_connection && g_SSEConnectionPool)
    {
        dmConnectionPool::Close(g_SSEConnectionPool, pool_connection);
    }
}

static bool SSEDM_HasControlChars(const char* value)
{
    if (!value)
    {
        return false;
    }

    for (const char* cursor = value; *cursor; ++cursor)
    {
        if (*cursor == '\r' || *cursor == '\n')
        {
            return true;
        }
    }

    return false;
}

static std::string SSEDM_LowerASCII(const std::string& value)
{
    std::string result;
    result.reserve(value.size());

    for (size_t i = 0; i < value.size(); ++i)
    {
        result.push_back((char)tolower((unsigned char)value[i]));
    }

    return result;
}

static bool SSEDM_IsDefaultPort(const SSEDMUrl* url)
{
    return (!url->m_Secure && url->m_Port == 80) || (url->m_Secure && url->m_Port == 443);
}

static bool SSEDM_ParsePort(const std::string& port, int* out_port)
{
    if (port.empty())
    {
        return false;
    }

    char* end = 0;
    errno = 0;
    const long parsed = strtol(port.c_str(), &end, 10);
    if (errno != 0 || !end || *end != 0 || parsed <= 0 || parsed > 65535)
    {
        return false;
    }

    *out_port = (int)parsed;
    return true;
}

static bool SSEDM_ParseUrl(const char* url, SSEDMUrl* output, char* error, uint32_t error_size)
{
    if (!url || !url[0])
    {
        dmSnPrintf(error, error_size, "SSE URL is empty");
        return false;
    }

    std::string value(url);
    const size_t scheme_end = value.find("://");
    if (scheme_end == std::string::npos)
    {
        dmSnPrintf(error, error_size, "SSE URL must include http:// or https://");
        return false;
    }

    const std::string scheme = SSEDM_LowerASCII(value.substr(0, scheme_end));
    if (scheme == "http")
    {
        output->m_Secure = 0;
        output->m_Port = 80;
    }
    else if (scheme == "https")
    {
        output->m_Secure = 1;
        output->m_Port = 443;
    }
    else
    {
        dmSnPrintf(error, error_size, "SSE URL must use http or https");
        return false;
    }

    const size_t authority_start = scheme_end + 3;
    const size_t path_start = value.find_first_of("/?#", authority_start);
    std::string authority;
    if (path_start == std::string::npos)
    {
        authority = value.substr(authority_start);
        output->m_Path = "/";
    }
    else
    {
        authority = value.substr(authority_start, path_start - authority_start);
        output->m_Path = value[path_start] == '/' ? value.substr(path_start) : "/" + value.substr(path_start);
    }

    const size_t fragment = output->m_Path.find('#');
    if (fragment != std::string::npos)
    {
        output->m_Path.resize(fragment);
    }
    if (output->m_Path.empty())
    {
        output->m_Path = "/";
    }

    if (authority.empty() || authority.find('@') != std::string::npos)
    {
        dmSnPrintf(error, error_size, "SSE URL host is invalid");
        return false;
    }

    if (authority[0] == '[')
    {
        const size_t end = authority.find(']');
        if (end == std::string::npos || end == 1)
        {
            dmSnPrintf(error, error_size, "SSE URL IPv6 host is invalid");
            return false;
        }

        output->m_Host = authority.substr(1, end - 1);
        if (end + 1 < authority.size())
        {
            if (authority[end + 1] != ':')
            {
                dmSnPrintf(error, error_size, "SSE URL host is invalid");
                return false;
            }
            if (!SSEDM_ParsePort(authority.substr(end + 2), &output->m_Port))
            {
                dmSnPrintf(error, error_size, "SSE URL port is invalid");
                return false;
            }
        }
    }
    else
    {
        const size_t first_colon = authority.find(':');
        const size_t last_colon = authority.rfind(':');
        if (first_colon != std::string::npos && first_colon == last_colon)
        {
            output->m_Host = authority.substr(0, first_colon);
            if (!SSEDM_ParsePort(authority.substr(first_colon + 1), &output->m_Port))
            {
                dmSnPrintf(error, error_size, "SSE URL port is invalid");
                return false;
            }
        }
        else
        {
            output->m_Host = authority;
        }
    }

    if (output->m_Host.empty())
    {
        dmSnPrintf(error, error_size, "SSE URL host is empty");
        return false;
    }

    const bool is_ipv6 = output->m_Host.find(':') != std::string::npos;
    output->m_HostHeader = is_ipv6 ? "[" + output->m_Host + "]" : output->m_Host;
    if (!SSEDM_IsDefaultPort(output))
    {
        char port[16];
        dmSnPrintf(port, sizeof(port), ":%d", output->m_Port);
        output->m_HostHeader += port;
    }

    return true;
}

static dmSocket::Result SSEDM_WaitForSocket(SSEDesktopConnection* connection, dmSocket::SelectorKind kind)
{
    while (!SSEDesktop_ShouldStop(connection))
    {
        dmSocket::Selector selector;
        dmSocket::SelectorZero(&selector);
        dmSocket::SelectorSet(&selector, kind, connection->m_Socket);

        dmSocket::Result result = dmSocket::Select(&selector, SSE_DM_SOCKET_WAIT_US);
        if (result != dmSocket::RESULT_WOULDBLOCK && result != dmSocket::RESULT_TRY_AGAIN)
        {
            return result;
        }
    }

    return dmSocket::RESULT_OK;
}

static dmSocket::Result SSEDM_SendRaw(SSEDesktopConnection* connection, const char* buffer, int length, int* sent_bytes)
{
    if (connection->m_SSLSocket)
    {
        return dmSSLSocket::Send(connection->m_SSLSocket, buffer, length, sent_bytes);
    }

    return dmSocket::Send(connection->m_Socket, buffer, length, sent_bytes);
}

static dmSocket::Result SSEDM_ReceiveRaw(SSEDesktopConnection* connection, void* buffer, int length, int* received_bytes)
{
    if (connection->m_SSLSocket)
    {
        return dmSSLSocket::Receive(connection->m_SSLSocket, buffer, length, received_bytes);
    }

    return dmSocket::Receive(connection->m_Socket, buffer, length, received_bytes);
}

static bool SSEDM_AppendHeader(std::string* request, const char* name, const char* value, char* error, uint32_t error_size)
{
    if (!name || !value)
    {
        return true;
    }
    if (!name[0] || SSEDM_HasControlChars(name) || SSEDM_HasControlChars(value) || strchr(name, ':') != 0)
    {
        dmSnPrintf(error, error_size, "invalid SSE request header");
        return false;
    }

    request->append(name);
    request->append(": ");
    request->append(value);
    request->append("\r\n");
    return true;
}

static bool SSEDM_BuildRequest(SSEDesktopConnection* connection, const SSEDMUrl* url, std::string* request, char* error, uint32_t error_size)
{
    request->clear();
    request->append("GET ");
    request->append(url->m_Path);
    request->append(" HTTP/1.1\r\n");
    request->append("Host: ");
    request->append(url->m_HostHeader);
    request->append("\r\n");
    request->append("Accept: text/event-stream\r\n");
    request->append("Cache-Control: no-cache\r\n");
    request->append("Connection: keep-alive\r\n");
    request->append("User-Agent: defold-extension-sse/0.2\r\n");

    for (uint32_t i = 0; i < connection->m_Headers.Size(); ++i)
    {
        if (!SSEDM_AppendHeader(request, connection->m_Headers[i].m_Name, connection->m_Headers[i].m_Value, error, error_size))
        {
            return false;
        }
    }

    if (connection->m_LastEventId && connection->m_LastEventId[0])
    {
        if (!SSEDM_AppendHeader(request, "Last-Event-ID", connection->m_LastEventId, error, error_size))
        {
            return false;
        }
    }

    request->append("\r\n");
    return true;
}

static bool SSEDM_SendAll(SSEDesktopConnection* connection, const std::string& request, char* error, uint32_t error_size)
{
    const char* data = request.data();
    int remaining = (int)request.size();

    while (remaining > 0 && !SSEDesktop_ShouldStop(connection))
    {
        int sent = 0;
        dmSocket::Result result = SSEDM_SendRaw(connection, data, remaining, &sent);
        if (result == dmSocket::RESULT_OK)
        {
            if (sent <= 0)
            {
                result = SSEDM_WaitForSocket(connection, dmSocket::SELECTOR_KIND_WRITE);
                if (result != dmSocket::RESULT_OK)
                {
                    dmSnPrintf(error, error_size, "socket send wait failed: %s", dmSocket::ResultToString(result));
                    return false;
                }
                continue;
            }

            data += sent;
            remaining -= sent;
            continue;
        }

        if (result == dmSocket::RESULT_WOULDBLOCK || result == dmSocket::RESULT_TRY_AGAIN)
        {
            result = SSEDM_WaitForSocket(connection, dmSocket::SELECTOR_KIND_WRITE);
            if (result != dmSocket::RESULT_OK)
            {
                dmSnPrintf(error, error_size, "socket send wait failed: %s", dmSocket::ResultToString(result));
                return false;
            }
            continue;
        }

        dmSnPrintf(error, error_size, "socket send failed: %s", dmSocket::ResultToString(result));
        return false;
    }

    return true;
}

static bool SSEDesktop_OnParserEvent(void* context, const SSEParsedEvent* event)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    const bool enqueued = SSE_EnqueueMessage(connection->m_Handle, event->m_Event, event->m_Data, event->m_Id);
    if (enqueued && event->m_LastEventId)
    {
        SSEDesktop_SetString(&connection->m_LastEventId, event->m_LastEventId);
        SSE_SetLastEventId(connection->m_Handle, event->m_LastEventId);
    }
    return enqueued;
}

static void SSEDesktop_OnParserRetry(void* context, int retry_ms)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    connection->m_RetryMS = retry_ms;
}

static void SSEDesktop_OnParserId(void* context, const char* id)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    SSEDesktop_SetString(&connection->m_LastEventId, id);
    SSE_SetLastEventId(connection->m_Handle, id);
}

static void SSEDesktop_OnParserError(void* context, const char* message)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    SSE_EnqueueError(connection->m_Handle, message, 0, connection->m_Reconnect != 0, connection->m_RetryMS);
}

static bool SSEDM_ParseChunkSize(const std::string& line, size_t* size)
{
    size_t value = 0;
    bool has_digit = false;

    for (size_t i = 0; i < line.size(); ++i)
    {
        const char c = line[i];
        if (c == ';')
        {
            break;
        }
        if (c == ' ' || c == '\t')
        {
            continue;
        }

        int digit = -1;
        if (c >= '0' && c <= '9')
        {
            digit = c - '0';
        }
        else if (c >= 'a' && c <= 'f')
        {
            digit = c - 'a' + 10;
        }
        else if (c >= 'A' && c <= 'F')
        {
            digit = c - 'A' + 10;
        }
        else
        {
            return false;
        }

        if (value > (SIZE_MAX - (size_t)digit) / 16)
        {
            return false;
        }
        value = value * 16 + (size_t)digit;
        has_digit = true;
    }

    if (!has_digit)
    {
        return false;
    }

    *size = value;
    return true;
}

static bool SSEDM_FeedChunkedBody(SSEDesktopConnection* connection, SSEDMChunkDecoder* decoder, const char* data, size_t size, const SSEParserCallbacks* callbacks, char* error, uint32_t error_size)
{
    size_t offset = 0;
    while (offset < size)
    {
        if (decoder->m_State == SSEDMChunkDecoder::STATE_DONE)
        {
            return true;
        }

        if (decoder->m_State == SSEDMChunkDecoder::STATE_SIZE)
        {
            const char c = data[offset++];
            decoder->m_Line.push_back(c);
            if (decoder->m_Line.size() > 1024)
            {
                dmSnPrintf(error, error_size, "chunk size line is too long");
                return false;
            }

            if (c == '\n')
            {
                while (!decoder->m_Line.empty() && (decoder->m_Line[decoder->m_Line.size() - 1] == '\n' || decoder->m_Line[decoder->m_Line.size() - 1] == '\r'))
                {
                    decoder->m_Line.resize(decoder->m_Line.size() - 1);
                }

                size_t chunk_size = 0;
                if (!SSEDM_ParseChunkSize(decoder->m_Line, &chunk_size))
                {
                    dmSnPrintf(error, error_size, "invalid chunk size");
                    return false;
                }

                decoder->m_Line.clear();
                decoder->m_Remaining = chunk_size;
                decoder->m_State = chunk_size == 0 ? SSEDMChunkDecoder::STATE_DONE : SSEDMChunkDecoder::STATE_DATA;
            }
            continue;
        }

        if (decoder->m_State == SSEDMChunkDecoder::STATE_DATA)
        {
            const size_t available = size - offset;
            const size_t chunk = decoder->m_Remaining < available ? decoder->m_Remaining : available;
            if (chunk > 0)
            {
                connection->m_Parser.Feed(data + offset, chunk, callbacks, connection);
                decoder->m_Remaining -= chunk;
                offset += chunk;
            }

            if (decoder->m_Remaining == 0)
            {
                decoder->m_State = SSEDMChunkDecoder::STATE_DATA_CRLF;
            }
            continue;
        }

        if (decoder->m_State == SSEDMChunkDecoder::STATE_DATA_CRLF)
        {
            const char c = data[offset++];
            if (c == '\r')
            {
                decoder->m_State = SSEDMChunkDecoder::STATE_DATA_LF;
            }
            else if (c == '\n')
            {
                decoder->m_State = SSEDMChunkDecoder::STATE_SIZE;
            }
            else
            {
                dmSnPrintf(error, error_size, "invalid chunk delimiter");
                return false;
            }
            continue;
        }

        if (decoder->m_State == SSEDMChunkDecoder::STATE_DATA_LF)
        {
            const char c = data[offset++];
            if (c != '\n')
            {
                dmSnPrintf(error, error_size, "invalid chunk delimiter");
                return false;
            }
            decoder->m_State = SSEDMChunkDecoder::STATE_SIZE;
        }
    }

    return true;
}

static size_t SSEDM_FindHeaderEnd(const std::string& response, size_t* delimiter_size)
{
    size_t end = response.find("\r\n\r\n");
    if (end != std::string::npos)
    {
        *delimiter_size = 4;
        return end;
    }

    end = response.find("\n\n");
    if (end != std::string::npos)
    {
        *delimiter_size = 2;
        return end;
    }

    return std::string::npos;
}

static std::string SSEDM_TrimTrailingCR(std::string line)
{
    if (!line.empty() && line[line.size() - 1] == '\r')
    {
        line.resize(line.size() - 1);
    }
    return line;
}

static bool SSEDM_ParseResponseHeaders(const std::string& headers, int32_t* status, bool* chunked, char* error, uint32_t error_size)
{
    *status = 0;
    *chunked = false;

    const size_t first_line_end = headers.find('\n');
    const std::string first_line = SSEDM_TrimTrailingCR(headers.substr(0, first_line_end));
    int parsed_status = 0;
    if (sscanf(first_line.c_str(), "HTTP/%*s %d", &parsed_status) != 1)
    {
        dmSnPrintf(error, error_size, "invalid HTTP response");
        return false;
    }
    *status = parsed_status;

    size_t line_start = first_line_end == std::string::npos ? headers.size() : first_line_end + 1;
    while (line_start < headers.size())
    {
        const size_t line_end = headers.find('\n', line_start);
        const size_t count = line_end == std::string::npos ? headers.size() - line_start : line_end - line_start;
        const std::string line = SSEDM_TrimTrailingCR(headers.substr(line_start, count));
        const std::string lower = SSEDM_LowerASCII(line);

        if (lower.find("transfer-encoding:") == 0 && lower.find("chunked") != std::string::npos)
        {
            *chunked = true;
        }

        if (line_end == std::string::npos)
        {
            break;
        }
        line_start = line_end + 1;
    }

    return true;
}

static bool SSEDM_FeedBody(SSEDesktopConnection* connection, bool chunked, SSEDMChunkDecoder* chunk_decoder, const char* data, size_t size, const SSEParserCallbacks* callbacks, char* error, uint32_t error_size)
{
    if (size == 0)
    {
        return true;
    }

    if (chunked)
    {
        return SSEDM_FeedChunkedBody(connection, chunk_decoder, data, size, callbacks, error, error_size);
    }

    connection->m_Parser.Feed(data, size, callbacks, connection);
    return true;
}

static bool SSEDM_ReadResponse(SSEDesktopConnection* connection, char* error, uint32_t error_size)
{
    char buffer[8192];
    std::string header_buffer;
    bool headers_done = false;
    bool chunked = false;
    SSEDMChunkDecoder chunk_decoder;

    SSEParserCallbacks callbacks;
    callbacks.m_OnEvent = SSEDesktop_OnParserEvent;
    callbacks.m_OnRetry = SSEDesktop_OnParserRetry;
    callbacks.m_OnId = SSEDesktop_OnParserId;
    callbacks.m_OnError = SSEDesktop_OnParserError;

    while (!SSEDesktop_ShouldStop(connection))
    {
        int received = 0;
        dmSocket::Result result = SSEDM_ReceiveRaw(connection, buffer, sizeof(buffer), &received);
        if (result == dmSocket::RESULT_WOULDBLOCK || result == dmSocket::RESULT_TRY_AGAIN)
        {
            result = SSEDM_WaitForSocket(connection, dmSocket::SELECTOR_KIND_READ);
            if (result != dmSocket::RESULT_OK)
            {
                dmSnPrintf(error, error_size, "socket read wait failed: %s", dmSocket::ResultToString(result));
                return false;
            }
            continue;
        }
        if (result != dmSocket::RESULT_OK)
        {
            if (SSEDesktop_ShouldStop(connection))
            {
                return true;
            }
            dmSnPrintf(error, error_size, "socket receive failed: %s", dmSocket::ResultToString(result));
            return false;
        }

        if (received == 0)
        {
            return true;
        }

        if (!headers_done)
        {
            header_buffer.append(buffer, (size_t)received);
            if (header_buffer.size() > 65536)
            {
                dmSnPrintf(error, error_size, "HTTP response headers are too large");
                return false;
            }

            size_t delimiter_size = 0;
            const size_t header_end = SSEDM_FindHeaderEnd(header_buffer, &delimiter_size);
            if (header_end == std::string::npos)
            {
                continue;
            }

            if (!SSEDM_ParseResponseHeaders(header_buffer.substr(0, header_end), &connection->m_Status, &chunked, error, error_size))
            {
                return false;
            }

            if (connection->m_Status < 200 || connection->m_Status >= 300)
            {
                dmSnPrintf(error, error_size, "SSE request failed with HTTP status %d", connection->m_Status);
                return false;
            }

            connection->m_Opened = 1;
            SSE_EnqueueOpen(connection->m_Handle, connection->m_Status);
            headers_done = true;

            const size_t body_start = header_end + delimiter_size;
            if (body_start < header_buffer.size())
            {
                if (!SSEDM_FeedBody(connection, chunked, &chunk_decoder, header_buffer.data() + body_start, header_buffer.size() - body_start, &callbacks, error, error_size))
                {
                    return false;
                }
                if (chunked && chunk_decoder.m_State == SSEDMChunkDecoder::STATE_DONE)
                {
                    return true;
                }
            }
            header_buffer.clear();
        }
        else
        {
            if (!SSEDM_FeedBody(connection, chunked, &chunk_decoder, buffer, (size_t)received, &callbacks, error, error_size))
            {
                return false;
            }
            if (chunked && chunk_decoder.m_State == SSEDMChunkDecoder::STATE_DONE)
            {
                return true;
            }
        }
    }

    return true;
}

static bool SSEDesktop_PerformOnce(SSEDesktopConnection* connection, char* error, uint32_t error_size)
{
    if (!g_SSEConnectionPool)
    {
        dmSnPrintf(error, error_size, "Defold connection pool is not initialized");
        return false;
    }

    SSEDMUrl url;
    if (!SSEDM_ParseUrl(connection->m_Url, &url, error, error_size))
    {
        return false;
    }

    connection->m_Parser.Reset();
    connection->m_Status = 0;
    connection->m_Opened = 0;
    connection->m_Secure = url.m_Secure;

    dmSocket::Result socket_result = dmSocket::RESULT_OK;
    dmConnectionPool::HConnection pool_connection = 0;
    connection->m_CancelFlag = 0;
    dmConnectionPool::Result pool_result = dmConnectionPool::Dial(g_SSEConnectionPool, url.m_Host.c_str(), url.m_Port, url.m_Secure != 0, SSE_DM_CONNECT_TIMEOUT_US, &connection->m_CancelFlag, &pool_connection, &socket_result);
    if (pool_result != dmConnectionPool::RESULT_OK)
    {
        dmSnPrintf(error, error_size, "failed to open SSE connection: %s", dmSocket::ResultToString(socket_result));
        return false;
    }

    SSEDM_SetConnection(connection, pool_connection);
    connection->m_Socket = dmConnectionPool::GetSocket(g_SSEConnectionPool, pool_connection);
    connection->m_SSLSocket = dmConnectionPool::GetSSLSocket(g_SSEConnectionPool, pool_connection);

    dmSocket::SetNoDelay(connection->m_Socket, true);
    dmSocket::SetBlocking(connection->m_Socket, false);
    dmSocket::SetReceiveTimeout(connection->m_Socket, 1000);
    if (connection->m_SSLSocket)
    {
        dmSSLSocket::SetReceiveTimeout(connection->m_SSLSocket, 1000);
    }

    if (SSEDesktop_ShouldStop(connection))
    {
        SSEDM_CloseActiveConnection(connection);
        return true;
    }

    std::string request;
    if (!SSEDM_BuildRequest(connection, &url, &request, error, error_size))
    {
        SSEDM_CloseActiveConnection(connection);
        return false;
    }

    if (!SSEDM_SendAll(connection, request, error, error_size))
    {
        SSEDM_CloseActiveConnection(connection);
        return false;
    }

    const bool read_ok = SSEDM_ReadResponse(connection, error, error_size);
    SSEDM_CloseActiveConnection(connection);

    if (SSEDesktop_ShouldStop(connection))
    {
        return true;
    }

    return read_ok;
}

static void SSEDesktop_SleepRetry(SSEDesktopConnection* connection)
{
    int32_t remaining = connection->m_RetryMS;
    while (remaining > 0 && !SSEDesktop_ShouldStop(connection))
    {
        const int32_t step = remaining > 100 ? 100 : remaining;
        dmTime::Sleep((uint32_t)step * 1000);
        remaining -= step;
    }
}

static void SSEDesktop_Worker(void* data)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)data;
    uint32_t attempt = 0;

    while (!SSEDesktop_ShouldStop(connection))
    {
        char error[256];
        error[0] = 0;
        ++attempt;

        SSE_DebugLog(
            "dmconnection attempt #%u start handle=%d reconnect=%d retry_ms=%d url=%s",
            attempt,
            connection->m_Handle,
            connection->m_Reconnect,
            connection->m_RetryMS,
            connection->m_Url ? connection->m_Url : "");

        const bool ok = SSEDesktop_PerformOnce(connection, error, sizeof(error));
        SSE_SetConnected(connection->m_Handle, false);

        SSE_DebugLog(
            "dmconnection attempt #%u finish handle=%d ok=%d status=%d reconnect=%d error=%s",
            attempt,
            connection->m_Handle,
            ok ? 1 : 0,
            connection->m_Status,
            connection->m_Reconnect,
            error[0] ? error : "");

        if (SSEDesktop_ShouldStop(connection))
        {
            break;
        }

        if (!ok)
        {
            SSE_EnqueueError(connection->m_Handle, error, connection->m_Status, connection->m_Reconnect != 0, connection->m_RetryMS);
        }

        if (!connection->m_Reconnect)
        {
            SSE_DebugLog("dmconnection worker stop handle=%d reconnect disabled", connection->m_Handle);
            break;
        }

        SSE_DebugLog("dmconnection retry sleep handle=%d retry_ms=%d", connection->m_Handle, connection->m_RetryMS);
        SSEDesktop_SleepRetry(connection);
    }

    if (!SSEDesktop_ShouldStop(connection))
    {
        SSE_EnqueueClosed(connection->m_Handle);
    }
}

bool SSE_Platform_Initialize()
{
    if (!g_SSEConnectionPool)
    {
        dmConnectionPool::Params pool_params;
        memset(&pool_params, 0, sizeof(pool_params));
        pool_params.m_MaxConnections = 16;

        dmConnectionPool::Result result = dmConnectionPool::New(&pool_params, &g_SSEConnectionPool);
        if (result != dmConnectionPool::RESULT_OK)
        {
            dmLogError("failed to create SSE connection pool: %d", result);
            g_SSEConnectionPool = 0;
            return false;
        }
    }

    return true;
}

void SSE_Platform_Finalize()
{
    if (g_SSEConnectionPool)
    {
        dmConnectionPool::Shutdown(g_SSEConnectionPool, dmSocket::SHUTDOWNTYPE_READWRITE);
        dmConnectionPool::Delete(g_SSEConnectionPool);
        g_SSEConnectionPool = 0;
    }
}

bool SSE_Platform_IsSupported()
{
    return g_SSEConnectionPool != 0;
}

bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size)
{
    if (!g_SSEConnectionPool)
    {
        dmSnPrintf(error, error_size, "Defold connection pool is not initialized");
        return false;
    }

    SSEDesktopConnection* desktop = new SSEDesktopConnection;
    desktop->m_Handle = connection->m_Handle;
    desktop->m_Url = SSEDesktop_StrDup(connection->m_Url);
    desktop->m_LastEventId = SSEDesktop_StrDup(connection->m_LastEventId);
    desktop->m_RetryMS = connection->m_RetryMS;
    desktop->m_Reconnect = connection->m_Reconnect;
    desktop->m_Mutex = dmMutex::New();

    for (uint32_t i = 0; i < connection->m_Headers.Size(); ++i)
    {
        if (desktop->m_Headers.Full())
        {
            desktop->m_Headers.OffsetCapacity(4);
        }

        SSEHeader header;
        header.m_Name = SSEDesktop_StrDup(connection->m_Headers[i].m_Name);
        header.m_Value = SSEDesktop_StrDup(connection->m_Headers[i].m_Value);
        desktop->m_Headers.Push(header);
    }

    connection->m_PlatformData = desktop;
    desktop->m_Thread = dmThread::New((dmThread::ThreadStart)SSEDesktop_Worker, 0x80000, desktop, "SSEConnect");
    if (!desktop->m_Thread)
    {
        connection->m_PlatformData = 0;
        dmSnPrintf(error, error_size, "failed to start SSE worker thread");
        for (uint32_t i = 0; i < desktop->m_Headers.Size(); ++i)
        {
            free(desktop->m_Headers[i].m_Name);
            free(desktop->m_Headers[i].m_Value);
        }
        dmMutex::Delete(desktop->m_Mutex);
        free(desktop->m_Url);
        free(desktop->m_LastEventId);
        delete desktop;
        return false;
    }

    return true;
}

void SSE_Platform_Disconnect(SSEConnection* connection)
{
    SSEDesktopConnection* desktop = (SSEDesktopConnection*)connection->m_PlatformData;
    if (!desktop)
    {
        return;
    }

    {
        DM_MUTEX_SCOPED_LOCK(desktop->m_Mutex);
        desktop->m_Stop = 1;
        desktop->m_CancelFlag = 1;
    }

    SSEDM_CloseActiveConnection(desktop);

    if (desktop->m_Thread)
    {
        dmThread::Join(desktop->m_Thread);
        desktop->m_Thread = 0;
    }

    for (uint32_t i = 0; i < desktop->m_Headers.Size(); ++i)
    {
        free(desktop->m_Headers[i].m_Name);
        free(desktop->m_Headers[i].m_Value);
    }

    if (desktop->m_Mutex)
    {
        dmMutex::Delete(desktop->m_Mutex);
    }

    free(desktop->m_Url);
    free(desktop->m_LastEventId);
    delete desktop;
    connection->m_PlatformData = 0;
}

bool SSE_Platform_IsConnected(SSEConnection* connection)
{
    SSEDesktopConnection* desktop = (SSEDesktopConnection*)connection->m_PlatformData;
    return desktop && desktop->m_Opened != 0 && !SSEDesktop_ShouldStop(desktop);
}

void SSE_Platform_Update()
{
}

#endif
