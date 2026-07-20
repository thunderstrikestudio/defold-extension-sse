#if defined(DM_PLATFORM_OSX) || defined(DM_PLATFORM_LINUX) || defined(DM_PLATFORM_WINDOWS)

#include "sse_private.h"
#include "sse_parser.h"

#include <dmsdk/dlib/thread.h>
#include <dmsdk/dlib/time.h>

#include <stdio.h>
#include <wchar.h>
#include <string>

#if defined(SSE_USE_LIBCURL)
#include "sse_curl_shim.h"
#elif defined(SSE_USE_WINHTTP)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#include <winhttp.h>
#endif

// Reconnect backoff parameters shared by both desktop implementations.
#define SSE_DESKTOP_RETRY_MIN_MS 1000
#define SSE_DESKTOP_RETRY_MAX_MS 300000
#define SSE_DESKTOP_MIN_DELAY_MS 500
#define SSE_DESKTOP_MAX_BACKOFF_SHIFT 8
// Bound on the time between starting a request and receiving response headers.
#define SSE_DESKTOP_FIRST_RESPONSE_TIMEOUT_US (30ull * 1000000ull)

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
    uint64_t m_AttemptStart;
    uint8_t m_Reconnect;
    uint8_t m_Stop;
    uint8_t m_Opened;
    uint8_t m_FirstResponseTimedOut;
    uint8_t m_Finished;

    SSEDesktopConnection()
    : m_Handle(0)
    , m_Url(0)
    , m_LastEventId(0)
    , m_Thread(0)
    , m_Mutex(0)
    , m_Curl(0)
    , m_RetryMS(3000)
    , m_Status(0)
    , m_AttemptStart(0)
    , m_Reconnect(0)
    , m_Stop(0)
    , m_Opened(0)
    , m_FirstResponseTimedOut(0)
    , m_Finished(0)
    {
    }
};

static uint8_t g_CurlInitialized = 0;

// Connections whose worker thread had not exited when disconnect was called.
// They are joined and freed from SSE_Platform_Update (engine main thread)
// once the worker signals completion, so disconnect never blocks a frame.
static dmArray<SSEDesktopConnection*> g_SSEDesktopZombies;
static dmMutex::HMutex g_SSEDesktopZombiesMutex = 0;

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

static void SSEDesktop_MarkFinished(SSEDesktopConnection* connection)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    connection->m_Finished = 1;
}

static bool SSEDesktop_IsFinished(SSEDesktopConnection* connection)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    return connection->m_Finished != 0;
}

static void SSEDesktop_Free(SSEDesktopConnection* desktop)
{
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
}

static int32_t SSEDesktop_ComputeRetryDelayMS(int32_t retry_ms, uint32_t failures)
{
    int64_t base = retry_ms;
    if (base < SSE_DESKTOP_RETRY_MIN_MS)
    {
        base = SSE_DESKTOP_RETRY_MIN_MS;
    }
    if (base > SSE_DESKTOP_RETRY_MAX_MS)
    {
        base = SSE_DESKTOP_RETRY_MAX_MS;
    }

    uint32_t shift = failures > 1 ? failures - 1 : 0;
    if (shift > SSE_DESKTOP_MAX_BACKOFF_SHIFT)
    {
        shift = SSE_DESKTOP_MAX_BACKOFF_SHIFT;
    }

    int64_t delay = base << shift;
    if (delay > SSE_DESKTOP_RETRY_MAX_MS)
    {
        delay = SSE_DESKTOP_RETRY_MAX_MS;
    }

    // +-20% jitter so reconnecting clients spread out instead of stampeding.
    const int64_t jitter = (int64_t)(dmTime::GetTime() % 401) - 200;
    delay += (delay * jitter) / 1000;
    if (delay < SSE_DESKTOP_MIN_DELAY_MS)
    {
        delay = SSE_DESKTOP_MIN_DELAY_MS;
    }

    return (int32_t)delay;
}

static bool SSEDesktop_OnParserEvent(void* context, const SSEParsedEvent* event)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    const bool enqueued = SSE_EnqueueMessage(connection->m_Handle, event->m_Event, event->m_Data, event->m_Id);
    if (enqueued && event->m_LastEventId)
    {
        // Commit the parser's persistent last-event-id buffer (which also
        // carries id-only checkpoints and empty spec-legal resets) only when
        // the event was actually delivered; otherwise a reconnect would skip
        // the events dropped on queue overflow. On a drop the parser rolls
        // the buffer back so a later commit cannot skip this event either.
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
    // An id-only checkpoint block carries no payload that could be dropped,
    // so it is committed as the resume position immediately.
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    SSEDesktop_SetString(&connection->m_LastEventId, id);
    SSE_SetLastEventId(connection->m_Handle, id);
}

static void SSEDesktop_OnParserError(void* context, const char* message)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    SSE_EnqueueError(connection->m_Handle, message, 0, connection->m_Reconnect != 0, connection->m_RetryMS);
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
    callbacks.m_OnError = SSEDesktop_OnParserError;
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
        // curl header callback data is not NUL-terminated; copy the status
        // line into a bounded buffer before letting sscanf scan it.
        char status_line[64];
        const size_t copy_length = total < sizeof(status_line) - 1 ? total : sizeof(status_line) - 1;
        memcpy(status_line, ptr, copy_length);
        status_line[copy_length] = 0;

        int status = 0;
        if (sscanf(status_line, "HTTP/%*s %d", &status) == 1)
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
    if (SSEDesktop_ShouldStop(connection))
    {
        return 1;
    }

    // Abort if the server accepted the connection but never finished sending
    // response headers; an opened stream is allowed to idle indefinitely.
    if (!connection->m_Opened && (dmTime::GetTime() - connection->m_AttemptStart) > SSE_DESKTOP_FIRST_RESPONSE_TIMEOUT_US)
    {
        connection->m_FirstResponseTimedOut = 1;
        return 1;
    }

    return 0;
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
    connection->m_FirstResponseTimedOut = 0;
    connection->m_AttemptStart = dmTime::GetTime();

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
        if (connection->m_FirstResponseTimedOut)
        {
            dmSnPrintf(error, error_size, "SSE timed out waiting for server response");
        }
        else
        {
            dmSnPrintf(error, error_size, "%s", curl_error[0] ? curl_error : curl_easy_strerror(result));
        }
        return false;
    }

    if (connection->m_Status < 200 || connection->m_Status >= 300)
    {
        dmSnPrintf(error, error_size, "SSE request failed with HTTP status %d", connection->m_Status);
        return false;
    }

    return true;
}

static void SSEDesktop_SleepRetry(SSEDesktopConnection* connection, int32_t delay_ms)
{
    int32_t remaining = delay_ms;
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
    uint32_t failures = 0;

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

        if (connection->m_Opened)
        {
            failures = 0;
        }

        if (!ok)
        {
            ++failures;
            SSE_EnqueueError(connection->m_Handle, error, connection->m_Status, connection->m_Reconnect != 0, connection->m_RetryMS);
        }

        if (!connection->m_Reconnect)
        {
            SSE_DebugLog("desktop worker stop handle=%d reconnect disabled", connection->m_Handle);
            break;
        }

        const int32_t delay_ms = SSEDesktop_ComputeRetryDelayMS(connection->m_RetryMS, failures);
        SSE_DebugLog("desktop retry sleep handle=%d delay_ms=%d failures=%u", connection->m_Handle, delay_ms, failures);
        SSEDesktop_SleepRetry(connection, delay_ms);
    }

    if (!SSEDesktop_ShouldStop(connection))
    {
        SSE_EnqueueClosed(connection->m_Handle);
    }

    // Must be the last access to the connection: once the finished flag is
    // set, the main thread may join and free it at any moment.
    SSEDesktop_MarkFinished(connection);
}

bool SSE_Platform_Initialize()
{
    if (!g_SSEDesktopZombiesMutex)
    {
        g_SSEDesktopZombiesMutex = dmMutex::New();
    }

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
    if (g_SSEDesktopZombiesMutex)
    {
        // Shutdown path: blocking joins are acceptable here. The workers have
        // their stop flag set and observe it via the progress callback.
        dmArray<SSEDesktopConnection*> zombies;
        {
            DM_MUTEX_SCOPED_LOCK(g_SSEDesktopZombiesMutex);
            zombies.Swap(g_SSEDesktopZombies);
        }

        for (uint32_t i = 0; i < zombies.Size(); ++i)
        {
            SSEDesktopConnection* desktop = zombies[i];
            if (desktop->m_Thread)
            {
                dmThread::Join(desktop->m_Thread);
                desktop->m_Thread = 0;
            }
            SSEDesktop_Free(desktop);
        }

        dmMutex::Delete(g_SSEDesktopZombiesMutex);
        g_SSEDesktopZombiesMutex = 0;
    }

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

    connection->m_PlatformData = 0;

    if (!desktop->m_Thread)
    {
        SSEDesktop_Free(desktop);
        return;
    }

    if (SSEDesktop_IsFinished(desktop))
    {
        // The worker already exited; joining returns immediately.
        dmThread::Join(desktop->m_Thread);
        desktop->m_Thread = 0;
        SSEDesktop_Free(desktop);
        return;
    }

    // Never block the engine main thread waiting for the network worker; it
    // is reaped from SSE_Platform_Update once it observes the stop flag.
    DM_MUTEX_SCOPED_LOCK(g_SSEDesktopZombiesMutex);
    if (g_SSEDesktopZombies.Full())
    {
        g_SSEDesktopZombies.OffsetCapacity(4);
    }
    g_SSEDesktopZombies.Push(desktop);
}

bool SSE_Platform_IsConnected(SSEConnection* connection)
{
    SSEDesktopConnection* desktop = (SSEDesktopConnection*)connection->m_PlatformData;
    return desktop && desktop->m_Opened != 0 && !SSEDesktop_ShouldStop(desktop);
}

void SSE_Platform_Update()
{
    if (!g_SSEDesktopZombiesMutex)
    {
        return;
    }

    DM_MUTEX_SCOPED_LOCK(g_SSEDesktopZombiesMutex);
    uint32_t i = 0;
    while (i < g_SSEDesktopZombies.Size())
    {
        SSEDesktopConnection* desktop = g_SSEDesktopZombies[i];
        if (SSEDesktop_IsFinished(desktop))
        {
            dmThread::Join(desktop->m_Thread);
            desktop->m_Thread = 0;
            SSEDesktop_Free(desktop);
            g_SSEDesktopZombies.EraseSwap(i);
        }
        else
        {
            ++i;
        }
    }
}

#elif defined(SSE_USE_WINHTTP)

struct SSEDesktopConnection
{
    int32_t m_Handle;
    char* m_Url;
    char* m_LastEventId;
    dmArray<SSEHeader> m_Headers;
    SSEParser m_Parser;
    dmThread::Thread m_Thread;
    dmMutex::HMutex m_Mutex;
    HINTERNET m_Session;
    HINTERNET m_Connect;
    HINTERNET m_Request;
    int32_t m_RetryMS;
    int32_t m_Status;
    uint8_t m_Reconnect;
    uint8_t m_Stop;
    uint8_t m_Opened;
    uint8_t m_Finished;

    SSEDesktopConnection()
    : m_Handle(0)
    , m_Url(0)
    , m_LastEventId(0)
    , m_Thread(0)
    , m_Mutex(0)
    , m_Session(0)
    , m_Connect(0)
    , m_Request(0)
    , m_RetryMS(3000)
    , m_Status(0)
    , m_Reconnect(0)
    , m_Stop(0)
    , m_Opened(0)
    , m_Finished(0)
    {
    }
};

// Connections whose worker thread had not exited when disconnect was called.
// They are joined and freed from SSE_Platform_Update (engine main thread)
// once the worker signals completion, so disconnect never blocks a frame.
static dmArray<SSEDesktopConnection*> g_SSEDesktopZombies;
static dmMutex::HMutex g_SSEDesktopZombiesMutex = 0;

// Bound on how long the worker can sit in a blocking body read before it
// wakes up; a receive timeout surfaces as a stream error and the normal
// reconnect logic resumes the stream with Last-Event-ID.
#define SSE_WINHTTP_BODY_RECEIVE_TIMEOUT_MS 300000
// Bound on WinHttpReceiveResponse (time to response headers).
#define SSE_WINHTTP_HEADER_RECEIVE_TIMEOUT_MS 30000

struct SSEWinHTTPUrl
{
    wchar_t* m_Host;
    wchar_t* m_Path;
    INTERNET_PORT m_Port;
    uint8_t m_Secure;

    SSEWinHTTPUrl()
    : m_Host(0)
    , m_Path(0)
    , m_Port(0)
    , m_Secure(0)
    {
    }
};

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

static void SSEDesktop_MarkFinished(SSEDesktopConnection* connection)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    connection->m_Finished = 1;
}

static bool SSEDesktop_IsFinished(SSEDesktopConnection* connection)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    return connection->m_Finished != 0;
}

static void SSEDesktop_Free(SSEDesktopConnection* desktop)
{
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
}

static int32_t SSEDesktop_ComputeRetryDelayMS(int32_t retry_ms, uint32_t failures)
{
    int64_t base = retry_ms;
    if (base < SSE_DESKTOP_RETRY_MIN_MS)
    {
        base = SSE_DESKTOP_RETRY_MIN_MS;
    }
    if (base > SSE_DESKTOP_RETRY_MAX_MS)
    {
        base = SSE_DESKTOP_RETRY_MAX_MS;
    }

    uint32_t shift = failures > 1 ? failures - 1 : 0;
    if (shift > SSE_DESKTOP_MAX_BACKOFF_SHIFT)
    {
        shift = SSE_DESKTOP_MAX_BACKOFF_SHIFT;
    }

    int64_t delay = base << shift;
    if (delay > SSE_DESKTOP_RETRY_MAX_MS)
    {
        delay = SSE_DESKTOP_RETRY_MAX_MS;
    }

    // +-20% jitter so reconnecting clients spread out instead of stampeding.
    const int64_t jitter = (int64_t)(dmTime::GetTime() % 401) - 200;
    delay += (delay * jitter) / 1000;
    if (delay < SSE_DESKTOP_MIN_DELAY_MS)
    {
        delay = SSE_DESKTOP_MIN_DELAY_MS;
    }

    return (int32_t)delay;
}

static void SSEWinHTTP_SetHandles(SSEDesktopConnection* connection, HINTERNET session, HINTERNET connect, HINTERNET request)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    connection->m_Session = session;
    connection->m_Connect = connect;
    connection->m_Request = request;
}

static void SSEWinHTTP_TakeHandles(SSEDesktopConnection* connection, HINTERNET* session, HINTERNET* connect, HINTERNET* request)
{
    DM_MUTEX_SCOPED_LOCK(connection->m_Mutex);
    *session = connection->m_Session;
    *connect = connection->m_Connect;
    *request = connection->m_Request;
    connection->m_Session = 0;
    connection->m_Connect = 0;
    connection->m_Request = 0;
}

static void SSEWinHTTP_CloseHandles(HINTERNET session, HINTERNET connect, HINTERNET request)
{
    if (request)
    {
        WinHttpCloseHandle(request);
    }
    if (connect)
    {
        WinHttpCloseHandle(connect);
    }
    if (session)
    {
        WinHttpCloseHandle(session);
    }
}

static void SSEWinHTTP_CloseActiveHandles(SSEDesktopConnection* connection)
{
    HINTERNET session = 0;
    HINTERNET connect = 0;
    HINTERNET request = 0;
    SSEWinHTTP_TakeHandles(connection, &session, &connect, &request);
    SSEWinHTTP_CloseHandles(session, connect, request);
}

static wchar_t* SSEWinHTTP_ToWide(const char* value)
{
    if (!value)
    {
        return 0;
    }

    const int length = MultiByteToWideChar(CP_UTF8, 0, value, -1, 0, 0);
    if (length <= 0)
    {
        return 0;
    }

    wchar_t* result = (wchar_t*)malloc((size_t)length * sizeof(wchar_t));
    if (!result)
    {
        return 0;
    }

    if (MultiByteToWideChar(CP_UTF8, 0, value, -1, result, length) <= 0)
    {
        free(result);
        return 0;
    }

    return result;
}

static wchar_t* SSEWinHTTP_DupWide(const wchar_t* value)
{
    if (!value)
    {
        return 0;
    }

    const size_t length = wcslen(value);
    wchar_t* result = (wchar_t*)malloc((length + 1) * sizeof(wchar_t));
    if (result)
    {
        memcpy(result, value, (length + 1) * sizeof(wchar_t));
    }
    return result;
}

static wchar_t* SSEWinHTTP_DupWideSegment(const wchar_t* value, DWORD length)
{
    wchar_t* result = (wchar_t*)malloc(((size_t)length + 1) * sizeof(wchar_t));
    if (!result)
    {
        return 0;
    }

    if (length > 0 && value)
    {
        memcpy(result, value, (size_t)length * sizeof(wchar_t));
    }
    result[length] = 0;
    return result;
}

static void SSEWinHTTP_FreeUrl(SSEWinHTTPUrl* url)
{
    free(url->m_Host);
    free(url->m_Path);
    url->m_Host = 0;
    url->m_Path = 0;
}

static void SSEWinHTTP_FormatLastError(char* error, uint32_t error_size, const char* prefix)
{
    const DWORD code = GetLastError();
    char message[256];
    message[0] = 0;

    FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        0,
        code,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        message,
        sizeof(message),
        0);

    for (size_t i = strlen(message); i > 0; --i)
    {
        const char c = message[i - 1];
        if (c == '\r' || c == '\n' || c == ' ')
        {
            message[i - 1] = 0;
        }
        else
        {
            break;
        }
    }

    dmSnPrintf(error, error_size, "%s failed: %s (%lu)", prefix, message[0] ? message : "WinHTTP error", (unsigned long)code);
}

static bool SSEWinHTTP_ParseUrl(const char* url, SSEWinHTTPUrl* output, char* error, uint32_t error_size)
{
    wchar_t* wide_url = SSEWinHTTP_ToWide(url);
    if (!wide_url)
    {
        dmSnPrintf(error, error_size, "failed to convert SSE URL to UTF-16");
        return false;
    }

    URL_COMPONENTS components;
    memset(&components, 0, sizeof(components));
    components.dwStructSize = sizeof(components);
    components.dwSchemeLength = (DWORD)-1;
    components.dwHostNameLength = (DWORD)-1;
    components.dwUrlPathLength = (DWORD)-1;
    components.dwExtraInfoLength = (DWORD)-1;

    if (!WinHttpCrackUrl(wide_url, 0, 0, &components))
    {
        SSEWinHTTP_FormatLastError(error, error_size, "WinHttpCrackUrl");
        free(wide_url);
        return false;
    }

    if (components.nScheme != INTERNET_SCHEME_HTTP && components.nScheme != INTERNET_SCHEME_HTTPS)
    {
        dmSnPrintf(error, error_size, "SSE URL must use http or https");
        free(wide_url);
        return false;
    }

    output->m_Host = SSEWinHTTP_DupWideSegment(components.lpszHostName, components.dwHostNameLength);
    output->m_Port = components.nPort;
    output->m_Secure = components.nScheme == INTERNET_SCHEME_HTTPS ? 1 : 0;

    std::wstring path;
    if (components.dwUrlPathLength > 0 && components.lpszUrlPath)
    {
        path.assign(components.lpszUrlPath, components.dwUrlPathLength);
    }
    else
    {
        path.assign(L"/");
    }

    if (components.dwExtraInfoLength > 0 && components.lpszExtraInfo)
    {
        path.append(components.lpszExtraInfo, components.dwExtraInfoLength);
    }
    output->m_Path = SSEWinHTTP_DupWide(path.c_str());

    free(wide_url);

    if (!output->m_Host || !output->m_Path)
    {
        SSEWinHTTP_FreeUrl(output);
        dmSnPrintf(error, error_size, "failed to allocate parsed SSE URL");
        return false;
    }

    return true;
}

static bool SSEWinHTTP_AddHeader(HINTERNET request, const char* name, const char* value, char* error, uint32_t error_size)
{
    if (!name || !value)
    {
        return true;
    }

    std::string header;
    header += name;
    header += ": ";
    header += value;

    wchar_t* wide_header = SSEWinHTTP_ToWide(header.c_str());
    if (!wide_header)
    {
        dmSnPrintf(error, error_size, "failed to convert request header to UTF-16");
        return false;
    }

    const BOOL ok = WinHttpAddRequestHeaders(request, wide_header, (DWORD)-1L, WINHTTP_ADDREQ_FLAG_ADD | WINHTTP_ADDREQ_FLAG_REPLACE);
    free(wide_header);

    if (!ok)
    {
        SSEWinHTTP_FormatLastError(error, error_size, "WinHttpAddRequestHeaders");
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
        // Commit the parser's persistent last-event-id buffer (which also
        // carries id-only checkpoints and empty spec-legal resets) only when
        // the event was actually delivered; otherwise a reconnect would skip
        // the events dropped on queue overflow. On a drop the parser rolls
        // the buffer back so a later commit cannot skip this event either.
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
    // An id-only checkpoint block carries no payload that could be dropped,
    // so it is committed as the resume position immediately.
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    SSEDesktop_SetString(&connection->m_LastEventId, id);
    SSE_SetLastEventId(connection->m_Handle, id);
}

static void SSEDesktop_OnParserError(void* context, const char* message)
{
    SSEDesktopConnection* connection = (SSEDesktopConnection*)context;
    SSE_EnqueueError(connection->m_Handle, message, 0, connection->m_Reconnect != 0, connection->m_RetryMS);
}

static bool SSEWinHTTP_ReadStream(SSEDesktopConnection* connection, HINTERNET request, char* error, uint32_t error_size)
{
    char buffer[8192];
    SSEParserCallbacks callbacks;
    callbacks.m_OnEvent = SSEDesktop_OnParserEvent;
    callbacks.m_OnRetry = SSEDesktop_OnParserRetry;
    callbacks.m_OnId = SSEDesktop_OnParserId;
    callbacks.m_OnError = SSEDesktop_OnParserError;

    while (!SSEDesktop_ShouldStop(connection))
    {
        DWORD available = 0;
        if (!WinHttpQueryDataAvailable(request, &available))
        {
            if (SSEDesktop_ShouldStop(connection))
            {
                return true;
            }
            SSEWinHTTP_FormatLastError(error, error_size, "WinHttpQueryDataAvailable");
            return false;
        }

        if (available == 0)
        {
            return true;
        }

        while (available > 0 && !SSEDesktop_ShouldStop(connection))
        {
            const DWORD chunk = available > sizeof(buffer) ? (DWORD)sizeof(buffer) : available;
            DWORD read = 0;
            if (!WinHttpReadData(request, buffer, chunk, &read))
            {
                if (SSEDesktop_ShouldStop(connection))
                {
                    return true;
                }
                SSEWinHTTP_FormatLastError(error, error_size, "WinHttpReadData");
                return false;
            }

            if (read == 0)
            {
                return true;
            }

            connection->m_Parser.Feed(buffer, read, &callbacks, connection);
            available -= read;
        }
    }

    return true;
}

static bool SSEDesktop_PerformOnce(SSEDesktopConnection* connection, char* error, uint32_t error_size)
{
    SSEWinHTTPUrl url;
    if (!SSEWinHTTP_ParseUrl(connection->m_Url, &url, error, error_size))
    {
        return false;
    }

    connection->m_Parser.Reset();
    connection->m_Status = 0;
    connection->m_Opened = 0;

    HINTERNET session = WinHttpOpen(
        L"defold-extension-sse/0.2",
        WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
        WINHTTP_NO_PROXY_NAME,
        WINHTTP_NO_PROXY_BYPASS,
        0);
    if (!session)
    {
        SSEWinHTTP_FormatLastError(error, error_size, "WinHttpOpen");
        SSEWinHTTP_FreeUrl(&url);
        return false;
    }
    SSEWinHTTP_SetHandles(connection, session, 0, 0);
    // The receive timeout bounds WinHttpReceiveResponse, i.e. the time until
    // response headers arrive; the body read gets a longer timeout below.
    WinHttpSetTimeouts(session, 15000, 15000, 15000, SSE_WINHTTP_HEADER_RECEIVE_TIMEOUT_MS);
    if (SSEDesktop_ShouldStop(connection))
    {
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return true;
    }

    HINTERNET connect = WinHttpConnect(session, url.m_Host, url.m_Port, 0);
    if (!connect)
    {
        SSEWinHTTP_FormatLastError(error, error_size, "WinHttpConnect");
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return false;
    }
    SSEWinHTTP_SetHandles(connection, session, connect, 0);
    if (SSEDesktop_ShouldStop(connection))
    {
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return true;
    }

    DWORD flags = WINHTTP_FLAG_REFRESH;
    if (url.m_Secure)
    {
        flags |= WINHTTP_FLAG_SECURE;
    }

    HINTERNET request = WinHttpOpenRequest(
        connect,
        L"GET",
        url.m_Path,
        0,
        WINHTTP_NO_REFERER,
        WINHTTP_DEFAULT_ACCEPT_TYPES,
        flags);
    if (!request)
    {
        SSEWinHTTP_FormatLastError(error, error_size, "WinHttpOpenRequest");
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return false;
    }
    SSEWinHTTP_SetHandles(connection, session, connect, request);
    if (SSEDesktop_ShouldStop(connection))
    {
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return true;
    }

    bool headers_ok = SSEWinHTTP_AddHeader(request, "Accept", "text/event-stream", error, error_size)
        && SSEWinHTTP_AddHeader(request, "Cache-Control", "no-cache", error, error_size);

    for (uint32_t i = 0; headers_ok && i < connection->m_Headers.Size(); ++i)
    {
        headers_ok = SSEWinHTTP_AddHeader(request, connection->m_Headers[i].m_Name, connection->m_Headers[i].m_Value, error, error_size);
    }

    if (headers_ok && connection->m_LastEventId && connection->m_LastEventId[0])
    {
        headers_ok = SSEWinHTTP_AddHeader(request, "Last-Event-ID", connection->m_LastEventId, error, error_size);
    }

    if (!headers_ok)
    {
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return false;
    }

    BOOL ok = WinHttpSendRequest(request, WINHTTP_NO_ADDITIONAL_HEADERS, 0, WINHTTP_NO_REQUEST_DATA, 0, 0, 0);
    if (!ok)
    {
        SSEWinHTTP_FormatLastError(error, error_size, "WinHttpSendRequest");
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return false;
    }

    ok = WinHttpReceiveResponse(request, 0);
    if (!ok)
    {
        SSEWinHTTP_FormatLastError(error, error_size, "WinHttpReceiveResponse");
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return false;
    }

    DWORD status = 0;
    DWORD status_size = sizeof(status);
    if (WinHttpQueryHeaders(request, WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER, WINHTTP_HEADER_NAME_BY_INDEX, &status, &status_size, WINHTTP_NO_HEADER_INDEX))
    {
        connection->m_Status = (int32_t)status;
    }

    if (connection->m_Status < 200 || connection->m_Status >= 300)
    {
        dmSnPrintf(error, error_size, "SSE request failed with HTTP status %d", connection->m_Status);
        SSEWinHTTP_CloseActiveHandles(connection);
        SSEWinHTTP_FreeUrl(&url);
        return false;
    }

    // Allow long idle periods on the open stream, but stay bounded so the
    // worker can observe the stop flag without handles being closed from
    // another thread; a timeout surfaces as a normal stream error/reconnect.
    DWORD body_receive_timeout = SSE_WINHTTP_BODY_RECEIVE_TIMEOUT_MS;
    WinHttpSetOption(request, WINHTTP_OPTION_RECEIVE_TIMEOUT, &body_receive_timeout, sizeof(body_receive_timeout));

    connection->m_Opened = 1;
    SSE_EnqueueOpen(connection->m_Handle, connection->m_Status);
    const bool stream_ok = SSEWinHTTP_ReadStream(connection, request, error, error_size);

    SSEWinHTTP_CloseActiveHandles(connection);
    SSEWinHTTP_FreeUrl(&url);

    if (SSEDesktop_ShouldStop(connection))
    {
        return true;
    }

    return stream_ok;
}

static void SSEDesktop_SleepRetry(SSEDesktopConnection* connection, int32_t delay_ms)
{
    int32_t remaining = delay_ms;
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
    uint32_t failures = 0;

    while (!SSEDesktop_ShouldStop(connection))
    {
        char error[256];
        error[0] = 0;
        ++attempt;

        SSE_DebugLog(
            "winhttp attempt #%u start handle=%d reconnect=%d retry_ms=%d url=%s",
            attempt,
            connection->m_Handle,
            connection->m_Reconnect,
            connection->m_RetryMS,
            connection->m_Url ? connection->m_Url : "");

        const bool ok = SSEDesktop_PerformOnce(connection, error, sizeof(error));
        SSE_SetConnected(connection->m_Handle, false);

        SSE_DebugLog(
            "winhttp attempt #%u finish handle=%d ok=%d status=%d reconnect=%d error=%s",
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

        if (connection->m_Opened)
        {
            failures = 0;
        }

        if (!ok)
        {
            ++failures;
            SSE_EnqueueError(connection->m_Handle, error, connection->m_Status, connection->m_Reconnect != 0, connection->m_RetryMS);
        }

        if (!connection->m_Reconnect)
        {
            SSE_DebugLog("winhttp worker stop handle=%d reconnect disabled", connection->m_Handle);
            break;
        }

        const int32_t delay_ms = SSEDesktop_ComputeRetryDelayMS(connection->m_RetryMS, failures);
        SSE_DebugLog("winhttp retry sleep handle=%d delay_ms=%d failures=%u", connection->m_Handle, delay_ms, failures);
        SSEDesktop_SleepRetry(connection, delay_ms);
    }

    if (!SSEDesktop_ShouldStop(connection))
    {
        SSE_EnqueueClosed(connection->m_Handle);
    }

    // Must be the last access to the connection: once the finished flag is
    // set, the main thread may join and free it at any moment.
    SSEDesktop_MarkFinished(connection);
}

bool SSE_Platform_Initialize()
{
    if (!g_SSEDesktopZombiesMutex)
    {
        g_SSEDesktopZombiesMutex = dmMutex::New();
    }
    return true;
}

void SSE_Platform_Finalize()
{
    if (!g_SSEDesktopZombiesMutex)
    {
        return;
    }

    // Shutdown path: blocking joins are acceptable here. As a last resort the
    // handles of a still-running worker are closed to abort its blocking
    // read; this cross-thread close is confined to app exit.
    dmArray<SSEDesktopConnection*> zombies;
    {
        DM_MUTEX_SCOPED_LOCK(g_SSEDesktopZombiesMutex);
        zombies.Swap(g_SSEDesktopZombies);
    }

    for (uint32_t i = 0; i < zombies.Size(); ++i)
    {
        SSEDesktopConnection* desktop = zombies[i];
        if (desktop->m_Thread)
        {
            if (!SSEDesktop_IsFinished(desktop))
            {
                SSEWinHTTP_CloseActiveHandles(desktop);
            }
            dmThread::Join(desktop->m_Thread);
            desktop->m_Thread = 0;
        }
        SSEDesktop_Free(desktop);
    }

    dmMutex::Delete(g_SSEDesktopZombiesMutex);
    g_SSEDesktopZombiesMutex = 0;
}

bool SSE_Platform_IsSupported()
{
    return true;
}

bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size)
{
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

    connection->m_PlatformData = 0;

    if (!desktop->m_Thread)
    {
        SSEDesktop_Free(desktop);
        return;
    }

    if (SSEDesktop_IsFinished(desktop))
    {
        // The worker already exited; joining returns immediately.
        dmThread::Join(desktop->m_Thread);
        desktop->m_Thread = 0;
        SSEDesktop_Free(desktop);
        return;
    }

    // Never block the engine main thread waiting for the network worker, and
    // never close WinHTTP handles that the worker may be using in a blocking
    // call: the worker unblocks itself via the receive timeouts, closes its
    // own handles, and is reaped from SSE_Platform_Update.
    DM_MUTEX_SCOPED_LOCK(g_SSEDesktopZombiesMutex);
    if (g_SSEDesktopZombies.Full())
    {
        g_SSEDesktopZombies.OffsetCapacity(4);
    }
    g_SSEDesktopZombies.Push(desktop);
}

bool SSE_Platform_IsConnected(SSEConnection* connection)
{
    SSEDesktopConnection* desktop = (SSEDesktopConnection*)connection->m_PlatformData;
    return desktop && desktop->m_Opened != 0 && !SSEDesktop_ShouldStop(desktop);
}

void SSE_Platform_Update()
{
    if (!g_SSEDesktopZombiesMutex)
    {
        return;
    }

    DM_MUTEX_SCOPED_LOCK(g_SSEDesktopZombiesMutex);
    uint32_t i = 0;
    while (i < g_SSEDesktopZombies.Size())
    {
        SSEDesktopConnection* desktop = g_SSEDesktopZombies[i];
        if (SSEDesktop_IsFinished(desktop))
        {
            dmThread::Join(desktop->m_Thread);
            desktop->m_Thread = 0;
            SSEDesktop_Free(desktop);
            g_SSEDesktopZombies.EraseSwap(i);
        }
        else
        {
            ++i;
        }
    }
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

void SSE_Platform_Update()
{
}

#endif

#endif
