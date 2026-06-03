#ifndef EXTENSION_SSE_PRIVATE_H
#define EXTENSION_SSE_PRIVATE_H

#include <dmsdk/sdk.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../include/sse.h"

#define SSE_LIB_NAME "SSEExt"
#define SSE_MODULE_NAME "sse"

#ifndef SSE_QUEUE_CAPACITY
#define SSE_QUEUE_CAPACITY 1024
#endif

struct SSEHeader
{
    char* m_Name;
    char* m_Value;

    SSEHeader()
    {
        memset(this, 0, sizeof(*this));
    }
};

struct SSEEvent
{
    int32_t m_Handle;
    int32_t m_Type;
    int32_t m_Status;
    int32_t m_RetryMS;
    uint8_t m_Reconnect;
    char* m_Event;
    char* m_Data;
    char* m_Id;
    char* m_Error;

    SSEEvent()
    {
        memset(this, 0, sizeof(*this));
    }
};

struct SSEConnection
{
    int32_t m_Handle;
    char* m_Url;
    char* m_LastEventId;
    int32_t m_RetryMS;
    uint8_t m_Reconnect;
    uint8_t m_Connected;
    uint8_t m_UserClosed;
    uint8_t m_Dispatching;
    uint8_t m_DestroyAfterDispatch;
    void* m_PlatformData;
    dmScript::LuaCallbackInfo* m_Callback;
    dmArray<SSEHeader> m_Headers;

    SSEConnection()
    {
        memset(this, 0, sizeof(*this));
        m_RetryMS = 3000;
    }
};

void SSE_EnqueueOpen(int32_t handle, int32_t status);
void SSE_EnqueueMessage(int32_t handle, const char* event_name, const char* data, const char* id);
void SSE_EnqueueError(int32_t handle, const char* error, int32_t status, bool reconnecting, int32_t retry_ms);
void SSE_EnqueueClosed(int32_t handle);

void SSE_SetConnected(int32_t handle, bool connected);
void SSE_SetLastEventId(int32_t handle, const char* last_event_id);
bool SSE_IsUserClosed(int32_t handle);
bool SSE_IsDebugEnabled();
void SSE_DebugLog(const char* format, ...);

bool SSE_Platform_Initialize();
void SSE_Platform_Finalize();
bool SSE_Platform_IsSupported();
bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size);
void SSE_Platform_Disconnect(SSEConnection* connection);
bool SSE_Platform_IsConnected(SSEConnection* connection);

#endif

