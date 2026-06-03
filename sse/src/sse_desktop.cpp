#if defined(DM_PLATFORM_OSX) || defined(DM_PLATFORM_LINUX) || defined(DM_PLATFORM_WINDOWS)

#include "sse_private.h"
#include "sse_parser.h"

#include <dmsdk/dlib/thread.h>
#include <dmsdk/dlib/time.h>

#include <stdio.h>

#if defined(SSE_USE_LIBCURL)
#include <curl/curl.h>
#endif

#if defined(SSE_USE_LIBCURL)

struct SSEDesktopConnection
{
    int32_t m_Handle;
    char* m_Url;
    char* m_LastEventId;
    dmArray<SSEHeader> m_Headers;
    SSEParser m_Parser;
    dmThread::Thread m_Thread;
    dmMutex::HMutex m_Mutex;
    CURL* m_Curl;
    int32_t m_RetryMS;
    int32_t m_Status;
    uint8_t m_Reconnect;
    uint8_t m_Stop;
    uint8_t m_Opened;

    SSEDesktopConnection()
    : m_Handle(0)
    , m_Url(0)
    , m_LastEventId(0)
    , m_Thread(0)
    , m_Mutex(0)
    , m_Curl(0)
    , m_RetryMS(3000)
    , m_Status(0)
    , m_Reconnect(0)
    , m_Stop(0)
    , m_Opened(0)
    {
    }
};

static uint8_t g_CurlInitialized = 0;

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

static void SSEDesktop_SetCurl(SSEDesktopConnection* connection, CURL* curl)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    connection->m_Curl = curl;
}

static void SSEDesktop_OnParserEvent(void* context, const SSEParsedEvent* event)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    if (event->m_Id && event->m_Id[0])
    {
        SSEDesktop_SetString(&connection->m_LastEventId, event->m_Id);
        SSE_SetLastEventId(connection->m_Handle, event->m_Id);
    }
    SSE_EnqueueMessage(connection->m_Handle, event->m_Event, event->m_Data, event->m_Id);
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

static size_t SSEDesktop_WriteCallback(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)userdata;
    const size_t total = size * nmemb;

    if (SSEDesktop_ShouldStop(connection))
    {
        return 0;
    }

    if (!connection->m_Opened && connection->m_Status >= 200 && connection->m_Status < 300)
    {
        connection->m_Opened = 1;
        SSE_EnqueueOpen(connection->m_Handle, connection->m_Status);
    }

    SSEParserCallbacks callbacks;
    callbacks.m_OnEvent = SSEDesktop_OnParserEvent;
    callbacks.m_OnRetry = SSEDesktop_OnParserRetry;
    callbacks.m_OnId = SSEDesktop_OnParserId;
    connection->m_Parser.Feed(ptr, total, &callbacks, connection);

    return total;
}

static size_t SSEDesktop_HeaderCallback(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)userdata;
    const size_t total = size * nmemb;

    if (SSEDesktop_ShouldStop(connection))
    {
        return 0;
    }

    if (total >= 5 && strncmp(ptr, "HTTP/", 5) == 0)
    {
        int status = 0;
        if (sscanf(ptr, "HTTP/%*s %d", &status) == 1)
        {
            connection->m_Status = status;
            connection->m_Opened = 0;
        }
    }
    else if ((total == 2 && ptr[0] == '\r' && ptr[1] == '\n') || (total == 1 && ptr[0] == '\n'))
    {
        if (!connection->m_Opened && connection->m_Status >= 200 && connection->m_Status < 300)
        {
            connection->m_Opened = 1;
            SSE_EnqueueOpen(connection->m_Handle, connection->m_Status);
        }
    }

    return total;
}

static int SSEDesktop_ProgressCallback(void* clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)clientp;
    return SSEDesktop_ShouldStop(connection) ? 1 : 0;
}

static void SSEDesktop_AddHeader(struct curl_slist** headers, const char* name, const char* value)
{
    if (!name || !value)
    {
        return;
    }

    const size_t length = strlen(name) + strlen(value) + 3;
    char* line = (char*)malloc(length);
    if (!line)
    {
        return;
    }

    dmSnPrintf(line, length, "%s: %s", name, value);
    *headers = curl_slist_append(*headers, line);
    free(line);
}

static bool SSEDesktop_PerformOnce(SSEDesktopConnection* connection, char* error, uint32_t error_size)
{
    CURL* curl = curl_easy_init();
    if (!curl)
    {
        dmSnPrintf(error, error_size, "curl_easy_init failed");
        return false;
    }

    connection->m_Parser.Reset();
    connection->m_Status = 0;
    connection->m_Opened = 0;

    struct curl_slist* headers = 0;
    headers = curl_slist_append(headers, "Accept: text/event-stream");
    headers = curl_slist_append(headers, "Cache-Control: no-cache");

    for (uint32_t i = 0; i < connection->m_Headers.Size(); ++i)
    {
        SSEDesktop_AddHeader(&headers, connection->m_Headers[i].m_Name, connection->m_Headers[i].m_Value);
    }

    if (connection->m_LastEventId && connection->m_LastEventId[0])
    {
        SSEDesktop_AddHeader(&headers, "Last-Event-ID", connection->m_LastEventId);
    }

    char curl_error[CURL_ERROR_SIZE];
    curl_error[0] = 0;

    curl_easy_setopt(curl, CURLOPT_URL, connection->m_Url);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, SSEDesktop_WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, connection);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, SSEDesktop_HeaderCallback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, connection);
    curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, SSEDesktop_ProgressCallback);
    curl_easy_setopt(curl, CURLOPT_XFERINFODATA, connection);
    curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, 15000L);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "defold-extension-sse/0.1");
    curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, curl_error);

    SSEDesktop_SetCurl(connection, curl);
    CURLcode result = curl_easy_perform(curl);

    long status = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    if (status > 0)
    {
        connection->m_Status = (int32_t)status;
    }

    SSEDesktop_SetCurl(connection, 0);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (SSEDesktop_ShouldStop(connection))
    {
        return true;
    }

    if (result != CURLE_OK)
    {
        dmSnPrintf(error, error_size, "%s", curl_error[0] ? curl_error : curl_easy_strerror(result));
        return false;
    }

    if (connection->m_Status < 200 || connection->m_Status >= 300)
    {
        dmSnPrintf(error, error_size, "SSE request failed with HTTP status %d", connection->m_Status);
        return false;
    }

    return true;
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
            "desktop attempt #%u start handle=%d reconnect=%d retry_ms=%d url=%s",
            attempt,
            connection->m_Handle,
            connection->m_Reconnect,
            connection->m_RetryMS,
            connection->m_Url ? connection->m_Url : "");

        const bool ok = SSEDesktop_PerformOnce(connection, error, sizeof(error));
        SSE_SetConnected(connection->m_Handle, false);

        SSE_DebugLog(
            "desktop attempt #%u finish handle=%d ok=%d status=%d reconnect=%d error=%s",
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
            SSE_DebugLog("desktop worker stop handle=%d reconnect disabled", connection->m_Handle);
            break;
        }

        SSE_DebugLog("desktop retry sleep handle=%d retry_ms=%d", connection->m_Handle, connection->m_RetryMS);
        SSEDesktop_SleepRetry(connection);
    }

    if (!SSEDesktop_ShouldStop(connection))
    {
        SSE_EnqueueClosed(connection->m_Handle);
    }
}

bool SSE_Platform_Initialize()
{
    if (!g_CurlInitialized)
    {
        const CURLcode result = curl_global_init(CURL_GLOBAL_DEFAULT);
        if (result != CURLE_OK)
        {
            dmLogError("curl_global_init failed: %s", curl_easy_strerror(result));
            return false;
        }
        g_CurlInitialized = 1;
    }

    return true;
}

void SSE_Platform_Finalize()
{
    if (g_CurlInitialized)
    {
        curl_global_cleanup();
        g_CurlInitialized = 0;
    }
}

bool SSE_Platform_IsSupported()
{
    return g_CurlInitialized != 0;
}

bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size)
{
    if (!g_CurlInitialized)
    {
        dmSnPrintf(error, error_size, "libcurl is not initialized");
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
    }

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

#else

bool SSE_Platform_Initialize()
{
    return true;
}

void SSE_Platform_Finalize()
{
}

bool SSE_Platform_IsSupported()
{
    return false;
}

bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size)
{
    dmSnPrintf(error, error_size, "desktop SSE support requires libcurl to be linked for this platform");
    return false;
}

void SSE_Platform_Disconnect(SSEConnection* connection)
{
    connection->m_PlatformData = 0;
}

bool SSE_Platform_IsConnected(SSEConnection* connection)
{
    return false;
}

#endif

#endif
