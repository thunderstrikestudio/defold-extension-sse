#include "sse_private.h"

#include <dmsdk/dlib/time.h>

#include <assert.h>
#include <atomic>
#include <stdarg.h>
#include <stdio.h>

struct SSEEventQueue
{
    dmArray<SSEEvent> m_Events;
    dmArray<SSEEvent> m_EventsSwap;
    dmArray<int32_t> m_OverflowHandles;
    dmArray<int32_t> m_OverflowSwap;
    dmMutex::HMutex m_Mutex;
};

struct SSEState
{
    dmArray<SSEConnection*> m_Connections;
    SSEEventQueue m_Queue;
    dmMutex::HMutex m_Mutex;
    int32_t m_NextHandle;
    uint8_t m_Debug;
    uint8_t m_Initialized;
};

static SSEState g_SSE;

// Guards cross-thread entry points against a torn-down queue/state: platform
// callbacks (NSURLSession, OkHttp, worker threads) may still be draining while
// AppFinalize deletes the mutexes. Entrants register themselves in
// g_SSEEntrants BEFORE checking the active flag, and AppFinalize clears the
// flag and then waits for the entrant count to reach zero before deleting the
// mutexes — so a thread that raced past the flag check is always waited out.
static std::atomic<bool> g_SSEActive(false);
static std::atomic<int32_t> g_SSEEntrants(0);

struct SSECallbackGuard
{
    bool m_Entered;

    SSECallbackGuard()
    {
        g_SSEEntrants.fetch_add(1);
        m_Entered = g_SSEActive.load();
        if (!m_Entered)
        {
            g_SSEEntrants.fetch_sub(1);
        }
    }

    ~SSECallbackGuard()
    {
        if (m_Entered)
        {
            g_SSEEntrants.fetch_sub(1);
        }
    }
};

static char* SSE_StrDup(const char* value)
{
    if (!value)
    {
        return 0;
    }

    const size_t length = strlen(value);
    char* copy = (char*)malloc(length + 1);
    if (!copy)
    {
        return 0;
    }

    memcpy(copy, value, length + 1);
    return copy;
}

static void SSE_FreeEvent(SSEEvent* event)
{
    free(event->m_Event);
    free(event->m_Data);
    free(event->m_Id);
    free(event->m_Error);
    memset(event, 0, sizeof(*event));
}

static void SSE_FreeHeaders(dmArray<SSEHeader>& headers)
{
    for (uint32_t i = 0; i < headers.Size(); ++i)
    {
        free(headers[i].m_Name);
        free(headers[i].m_Value);
    }
    headers.SetSize(0);
}

static void SSE_AddHeader(SSEConnection* connection, const char* name, const char* value)
{
    if (!name || !value)
    {
        return;
    }

    if (connection->m_Headers.Full())
    {
        connection->m_Headers.OffsetCapacity(4);
    }

    SSEHeader header;
    header.m_Name = SSE_StrDup(name);
    header.m_Value = SSE_StrDup(value);
    connection->m_Headers.Push(header);
}

static SSEConnection* SSE_FindConnectionNoLock(int32_t handle)
{
    for (uint32_t i = 0; i < g_SSE.m_Connections.Size(); ++i)
    {
        SSEConnection* connection = g_SSE.m_Connections[i];
        if (connection && connection->m_Handle == handle)
        {
            return connection;
        }
    }

    return 0;
}

static SSEConnection* SSE_FindConnection(int32_t handle)
{
    DM_MUTEX_SCOPED_LOCK(g_SSE.m_Mutex);
    return SSE_FindConnectionNoLock(handle);
}

static void SSE_RemoveConnectionNoLock(SSEConnection* connection)
{
    for (uint32_t i = 0; i < g_SSE.m_Connections.Size(); ++i)
    {
        if (g_SSE.m_Connections[i] == connection)
        {
            g_SSE.m_Connections.EraseSwap(i);
            return;
        }
    }
}

static void SSE_DestroyConnection(SSEConnection* connection, bool disconnect_platform)
{
    if (!connection)
    {
        return;
    }

    if (disconnect_platform && connection->m_PlatformData)
    {
        SSE_Platform_Disconnect(connection);
    }

    {
        DM_MUTEX_SCOPED_LOCK(g_SSE.m_Mutex);
        SSE_RemoveConnectionNoLock(connection);
    }

    if (connection->m_Callback)
    {
        dmScript::DestroyCallback(connection->m_Callback);
        connection->m_Callback = 0;
    }

    SSE_FreeHeaders(connection->m_Headers);
    free(connection->m_Url);
    free(connection->m_LastEventId);
    delete connection;
}

static bool SSE_QueueHasOverflowHandleNoLock(int32_t handle)
{
    for (uint32_t i = 0; i < g_SSE.m_Queue.m_OverflowHandles.Size(); ++i)
    {
        if (g_SSE.m_Queue.m_OverflowHandles[i] == handle)
        {
            return true;
        }
    }

    return false;
}

static bool SSE_QueuePush(SSEEvent* event)
{
    SSECallbackGuard guard;
    if (!guard.m_Entered)
    {
        SSE_FreeEvent(event);
        return false;
    }

    DM_MUTEX_SCOPED_LOCK(g_SSE.m_Queue.m_Mutex);

    if (g_SSE.m_Queue.m_Events.Full())
    {
        if (!SSE_QueueHasOverflowHandleNoLock(event->m_Handle))
        {
            if (g_SSE.m_Queue.m_OverflowHandles.Full())
            {
                g_SSE.m_Queue.m_OverflowHandles.OffsetCapacity(4);
            }
            g_SSE.m_Queue.m_OverflowHandles.Push(event->m_Handle);
        }

        SSE_FreeEvent(event);
        return false;
    }

    g_SSE.m_Queue.m_Events.Push(*event);
    return true;
}

void SSE_EnqueueOpen(int32_t handle, int32_t status)
{
    SSE_SetConnected(handle, true);

    SSEEvent event;
    event.m_Handle = handle;
    event.m_Type = SSE_EVENT_OPEN;
    event.m_Status = status;
    SSE_QueuePush(&event);
}

bool SSE_EnqueueMessage(int32_t handle, const char* event_name, const char* data, const char* id)
{
    SSEEvent event;
    event.m_Handle = handle;
    event.m_Type = SSE_EVENT_MESSAGE;
    event.m_Event = SSE_StrDup(event_name ? event_name : "message");
    event.m_Data = SSE_StrDup(data ? data : "");
    event.m_Id = SSE_StrDup(id ? id : "");
    return SSE_QueuePush(&event);
}

void SSE_EnqueueError(int32_t handle, const char* error, int32_t status, bool reconnecting, int32_t retry_ms)
{
    SSEEvent event;
    event.m_Handle = handle;
    event.m_Type = SSE_EVENT_ERROR;
    event.m_Status = status;
    event.m_RetryMS = retry_ms;
    event.m_Reconnect = reconnecting ? 1 : 0;
    event.m_Error = SSE_StrDup(error ? error : "SSE error");
    SSE_QueuePush(&event);
}

void SSE_EnqueueClosed(int32_t handle)
{
    SSE_SetConnected(handle, false);

    SSEEvent event;
    event.m_Handle = handle;
    event.m_Type = SSE_EVENT_CLOSED;
    SSE_QueuePush(&event);
}

void SSE_SetConnected(int32_t handle, bool connected)
{
    SSECallbackGuard guard;
    if (!guard.m_Entered)
    {
        return;
    }

    DM_MUTEX_SCOPED_LOCK(g_SSE.m_Mutex);
    SSEConnection* connection = SSE_FindConnectionNoLock(handle);
    if (connection)
    {
        connection->m_Connected = connected ? 1 : 0;
    }
}

void SSE_SetLastEventId(int32_t handle, const char* last_event_id)
{
    SSECallbackGuard guard;
    if (!guard.m_Entered)
    {
        return;
    }

    DM_MUTEX_SCOPED_LOCK(g_SSE.m_Mutex);
    SSEConnection* connection = SSE_FindConnectionNoLock(handle);
    if (connection)
    {
        free(connection->m_LastEventId);
        connection->m_LastEventId = SSE_StrDup(last_event_id ? last_event_id : "");
    }
}

bool SSE_IsUserClosed(int32_t handle)
{
    SSECallbackGuard guard;
    if (!guard.m_Entered)
    {
        return true;
    }

    DM_MUTEX_SCOPED_LOCK(g_SSE.m_Mutex);
    SSEConnection* connection = SSE_FindConnectionNoLock(handle);
    return !connection || connection->m_UserClosed != 0;
}

bool SSE_IsDebugEnabled()
{
    return g_SSE.m_Debug != 0;
}

void SSE_DebugLog(const char* format, ...)
{
    if (!g_SSE.m_Debug)
    {
        return;
    }

    char buffer[1024];
    va_list args;
    va_start(args, format);
    vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    dmLogInfo("[sse] %s", buffer);
}

static void SSE_PushConstants(lua_State* L)
{
    lua_pushinteger(L, SSE_EVENT_OPEN);
    lua_setfield(L, -2, "EVENT_OPEN");
    lua_pushinteger(L, SSE_EVENT_MESSAGE);
    lua_setfield(L, -2, "EVENT_MESSAGE");
    lua_pushinteger(L, SSE_EVENT_ERROR);
    lua_setfield(L, -2, "EVENT_ERROR");
    lua_pushinteger(L, SSE_EVENT_CLOSED);
    lua_setfield(L, -2, "EVENT_CLOSED");
}

static void SSE_PushEventTable(lua_State* L, const SSEEvent* event)
{
    lua_newtable(L);

    lua_pushinteger(L, event->m_Type);
    lua_setfield(L, -2, "type");

    lua_pushinteger(L, event->m_Handle);
    lua_setfield(L, -2, "handle");

    if (event->m_Status > 0)
    {
        lua_pushinteger(L, event->m_Status);
        lua_setfield(L, -2, "status");
    }

    if (event->m_Type == SSE_EVENT_MESSAGE)
    {
        lua_pushstring(L, event->m_Event ? event->m_Event : "message");
        lua_setfield(L, -2, "event");
        lua_pushstring(L, event->m_Data ? event->m_Data : "");
        lua_setfield(L, -2, "data");
        lua_pushstring(L, event->m_Id ? event->m_Id : "");
        lua_setfield(L, -2, "id");
    }
    else if (event->m_Type == SSE_EVENT_ERROR)
    {
        lua_pushstring(L, event->m_Error ? event->m_Error : "SSE error");
        lua_setfield(L, -2, "error");
        lua_pushboolean(L, event->m_Reconnect != 0);
        lua_setfield(L, -2, "reconnect");
        if (event->m_RetryMS >= 0)
        {
            lua_pushinteger(L, event->m_RetryMS);
            lua_setfield(L, -2, "retry_ms");
        }
    }
}

static void SSE_DispatchEvent(const SSEEvent* event)
{
    SSEConnection* connection = SSE_FindConnection(event->m_Handle);
    if (!connection || !connection->m_Callback || connection->m_UserClosed)
    {
        return;
    }

    if (!dmScript::IsCallbackValid(connection->m_Callback))
    {
        // The owning script instance died without calling sse.disconnect; tear
        // the connection down so the network thread stops reconnecting forever.
        SSE_DebugLog("callback no longer valid; destroying orphaned connection handle=%d", connection->m_Handle);
        connection->m_UserClosed = 1;
        SSE_DestroyConnection(connection, true);
        return;
    }

    lua_State* L = dmScript::GetCallbackLuaContext(connection->m_Callback);
    int top = lua_gettop(L);
    connection->m_Dispatching = 1;

    if (dmScript::SetupCallback(connection->m_Callback))
    {
        SSE_PushEventTable(L, event);
        dmScript::PCall(L, 2, 0);
        dmScript::TeardownCallback(connection->m_Callback);
    }

    const bool destroy_after_dispatch = connection->m_DestroyAfterDispatch != 0;
    connection->m_Dispatching = 0;
    assert(top == lua_gettop(L));

    if (destroy_after_dispatch)
    {
        SSE_DestroyConnection(connection, false);
    }
}

static void SSE_DispatchOverflow(int32_t handle)
{
    SSEEvent event;
    event.m_Handle = handle;
    event.m_Type = SSE_EVENT_ERROR;
    event.m_Status = 0;
    event.m_RetryMS = -1;
    event.m_Error = SSE_StrDup("SSE event queue overflow; events were dropped");

    SSE_DispatchEvent(&event);
    SSE_FreeEvent(&event);
}

static void SSE_FlushQueue()
{
    // Swap with persistent scratch arrays so the backing buffers are reused
    // between updates instead of being freed and re-allocated every frame.
    dmArray<SSEEvent>& events = g_SSE.m_Queue.m_EventsSwap;
    dmArray<int32_t>& overflow_handles = g_SSE.m_Queue.m_OverflowSwap;

    {
        DM_MUTEX_SCOPED_LOCK(g_SSE.m_Queue.m_Mutex);
        g_SSE.m_Queue.m_Events.Swap(events);
        g_SSE.m_Queue.m_OverflowHandles.Swap(overflow_handles);
    }

    for (uint32_t i = 0; i < events.Size(); ++i)
    {
        SSE_DispatchEvent(&events[i]);
        SSE_FreeEvent(&events[i]);
    }
    events.SetSize(0);

    for (uint32_t i = 0; i < overflow_handles.Size(); ++i)
    {
        SSE_DispatchOverflow(overflow_handles[i]);
    }
    overflow_handles.SetSize(0);
}

// Reads the options table without raising Lua errors: a longjmp here would
// leak the connection struct and its callback ref (which pins the script
// instance). Returns false with a message in error instead.
static bool SSE_ReadOptions(lua_State* L, int options_index, SSEConnection* connection, char* error, uint32_t error_size)
{
    if (options_index <= 0 || lua_isnil(L, options_index))
    {
        return true;
    }

    if (!lua_istable(L, options_index))
    {
        dmSnPrintf(error, error_size, "sse.connect options must be a table");
        return false;
    }

    lua_getfield(L, options_index, "headers");
    if (!lua_isnil(L, -1))
    {
        if (!lua_istable(L, -1))
        {
            lua_pop(L, 1);
            dmSnPrintf(error, error_size, "sse.connect options.headers must be a table");
            return false;
        }
        lua_pushnil(L);
        while (lua_next(L, -2) != 0)
        {
            if (lua_type(L, -2) == LUA_TSTRING)
            {
                const char* name = lua_tostring(L, -2);
                const char* value = lua_tostring(L, -1);
                if (name && value)
                {
                    SSE_AddHeader(connection, name, value);
                }
            }
            lua_pop(L, 1);
        }
    }
    lua_pop(L, 1);

    lua_getfield(L, options_index, "last_event_id");
    if (!lua_isnil(L, -1))
    {
        const char* last_event_id = lua_tostring(L, -1);
        if (!last_event_id)
        {
            lua_pop(L, 1);
            dmSnPrintf(error, error_size, "sse.connect options.last_event_id must be a string");
            return false;
        }
        free(connection->m_LastEventId);
        connection->m_LastEventId = SSE_StrDup(last_event_id);
    }
    lua_pop(L, 1);

    lua_getfield(L, options_index, "reconnect");
    if (!lua_isnil(L, -1))
    {
        connection->m_Reconnect = lua_toboolean(L, -1) ? 1 : 0;
    }
    lua_pop(L, 1);

    lua_getfield(L, options_index, "retry_ms");
    if (!lua_isnil(L, -1))
    {
        if (!lua_isnumber(L, -1))
        {
            lua_pop(L, 1);
            dmSnPrintf(error, error_size, "sse.connect options.retry_ms must be a number");
            return false;
        }
        const int retry_ms = (int)lua_tointeger(L, -1);
        connection->m_RetryMS = retry_ms >= 0 ? retry_ms : 0;
    }
    lua_pop(L, 1);

    return true;
}

static int SSE_Connect(lua_State* L)
{
    if (!SSE_Platform_IsSupported())
    {
        lua_pushnil(L);
        lua_pushstring(L, "sse is not supported on this platform");
        return 2;
    }

    const char* url = luaL_checkstring(L, 1);
    int callback_index = 3;
    int options_index = 2;

    if (lua_isfunction(L, 2))
    {
        callback_index = 2;
        options_index = 0;
    }

    luaL_checktype(L, callback_index, LUA_TFUNCTION);

    SSEConnection* connection = new SSEConnection;
    connection->m_Handle = g_SSE.m_NextHandle++;
    connection->m_Url = SSE_StrDup(url);
    connection->m_Callback = dmScript::CreateCallback(L, callback_index);

    if (!connection->m_Callback || !dmScript::IsCallbackValid(connection->m_Callback))
    {
        SSE_DestroyConnection(connection, false);
        lua_pushnil(L);
        lua_pushstring(L, "failed to create sse callback");
        return 2;
    }

    char error[256];
    error[0] = 0;
    if (!SSE_ReadOptions(L, options_index, connection, error, sizeof(error)))
    {
        SSE_DestroyConnection(connection, false);
        lua_pushnil(L);
        lua_pushstring(L, error[0] ? error : "invalid sse.connect options");
        return 2;
    }

    {
        DM_MUTEX_SCOPED_LOCK(g_SSE.m_Mutex);
        if (g_SSE.m_Connections.Full())
        {
            g_SSE.m_Connections.OffsetCapacity(4);
        }
        g_SSE.m_Connections.Push(connection);
    }

    if (!SSE_Platform_Connect(connection, error, sizeof(error)))
    {
        SSE_DestroyConnection(connection, false);
        lua_pushnil(L);
        lua_pushstring(L, error[0] ? error : "failed to connect sse");
        return 2;
    }

    lua_pushinteger(L, connection->m_Handle);
    return 1;
}

static int SSE_Disconnect(lua_State* L)
{
    DM_LUA_STACK_CHECK(L, 0);

    const int32_t handle = (int32_t)luaL_checkinteger(L, 1);
    SSEConnection* connection = SSE_FindConnection(handle);
    if (!connection || connection->m_UserClosed)
    {
        return 0;
    }

    connection->m_UserClosed = 1;
    connection->m_Connected = 0;

    if (connection->m_PlatformData)
    {
        SSE_Platform_Disconnect(connection);
    }

    if (connection->m_Dispatching)
    {
        connection->m_DestroyAfterDispatch = 1;
    }
    else
    {
        SSE_DestroyConnection(connection, false);
    }

    return 0;
}

static int SSE_IsSupported(lua_State* L)
{
    DM_LUA_STACK_CHECK(L, 1);
    lua_pushboolean(L, SSE_Platform_IsSupported());
    return 1;
}

static int SSE_IsConnected(lua_State* L)
{
    DM_LUA_STACK_CHECK(L, 1);

    const int32_t handle = (int32_t)luaL_checkinteger(L, 1);
    SSEConnection* connection = SSE_FindConnection(handle);
    lua_pushboolean(L, connection && connection->m_Connected && SSE_Platform_IsConnected(connection));
    return 1;
}

static int SSE_SetDebug(lua_State* L)
{
    DM_LUA_STACK_CHECK(L, 0);
    g_SSE.m_Debug = lua_toboolean(L, 1) ? 1 : 0;
    return 0;
}

static const luaL_reg SSE_methods[] =
{
    {"connect", SSE_Connect},
    {"disconnect", SSE_Disconnect},
    {"is_supported", SSE_IsSupported},
    {"is_connected", SSE_IsConnected},
    {"set_debug", SSE_SetDebug},
    {0, 0}
};

static void SSE_LuaInit(lua_State* L)
{
    int top = lua_gettop(L);
    luaL_register(L, SSE_MODULE_NAME, SSE_methods);
    SSE_PushConstants(L);
    lua_pop(L, 1);
    assert(top == lua_gettop(L));
}

static dmExtension::Result AppInitializeSSE(dmExtension::AppParams* params)
{
    memset(&g_SSE, 0, sizeof(g_SSE));
    g_SSE.m_NextHandle = 1;
    g_SSE.m_Debug = 0;
    g_SSE.m_Initialized = 1;
    g_SSE.m_Mutex = dmMutex::New();
    g_SSE.m_Queue.m_Mutex = dmMutex::New();
    g_SSE.m_Queue.m_Events.SetCapacity(SSE_QUEUE_CAPACITY);
    g_SSE.m_Queue.m_EventsSwap.SetCapacity(SSE_QUEUE_CAPACITY);
    g_SSEActive.store(true);
    return dmExtension::RESULT_OK;
}

static dmExtension::Result InitializeSSE(dmExtension::Params* params)
{
    if (!SSE_Platform_Initialize())
    {
        dmLogWarning("SSE platform adapter did not initialize; sse.is_supported() will report false");
    }

    SSE_LuaInit(params->m_L);
    return dmExtension::RESULT_OK;
}

static dmExtension::Result UpdateSSE(dmExtension::Params* params)
{
    SSE_FlushQueue();
    SSE_Platform_Update();
    return dmExtension::RESULT_OK;
}

static dmExtension::Result FinalizeSSE(dmExtension::Params* params)
{
    while (g_SSE.m_Connections.Size() > 0)
    {
        SSE_DestroyConnection(g_SSE.m_Connections[0], true);
    }

    SSE_FlushQueue();
    SSE_Platform_Finalize();
    return dmExtension::RESULT_OK;
}

static dmExtension::Result AppFinalizeSSE(dmExtension::AppParams* params)
{
    // Stop accepting cross-thread pushes, then wait until every entrant that
    // raced past the active check has left its (short, lock-bounded) critical
    // section before the mutexes are deleted. Platform adapters were already
    // quiesced in FinalizeSSE, so this converges immediately in practice.
    g_SSEActive.store(false);
    while (g_SSEEntrants.load() != 0)
    {
        dmTime::Sleep(1000);
    }

    if (g_SSE.m_Queue.m_Mutex)
    {
        {
            DM_MUTEX_SCOPED_LOCK(g_SSE.m_Queue.m_Mutex);
            for (uint32_t i = 0; i < g_SSE.m_Queue.m_Events.Size(); ++i)
            {
                SSE_FreeEvent(&g_SSE.m_Queue.m_Events[i]);
            }
            // Empty the arrays before shrinking: dmArray requires the size to
            // stay within the capacity, and a late platform callback may have
            // enqueued after the final flush.
            g_SSE.m_Queue.m_Events.SetSize(0);
            g_SSE.m_Queue.m_OverflowHandles.SetSize(0);
            g_SSE.m_Queue.m_Events.SetCapacity(0);
            g_SSE.m_Queue.m_EventsSwap.SetCapacity(0);
            g_SSE.m_Queue.m_OverflowHandles.SetCapacity(0);
            g_SSE.m_Queue.m_OverflowSwap.SetCapacity(0);
        }
        dmMutex::Delete(g_SSE.m_Queue.m_Mutex);
        g_SSE.m_Queue.m_Mutex = 0;
    }

    if (g_SSE.m_Mutex)
    {
        {
            DM_MUTEX_SCOPED_LOCK(g_SSE.m_Mutex);
            g_SSE.m_Connections.SetSize(0);
            g_SSE.m_Connections.SetCapacity(0);
        }
        dmMutex::Delete(g_SSE.m_Mutex);
        g_SSE.m_Mutex = 0;
    }

    memset(&g_SSE, 0, sizeof(g_SSE));
    return dmExtension::RESULT_OK;
}

DM_DECLARE_EXTENSION(SSEExt, "SSE", AppInitializeSSE, AppFinalizeSSE, InitializeSSE, UpdateSSE, 0, FinalizeSSE)
