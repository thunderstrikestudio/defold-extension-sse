#if !defined(DM_PLATFORM_OSX) && !defined(DM_PLATFORM_LINUX) && !defined(DM_PLATFORM_WINDOWS) && !defined(DM_PLATFORM_ANDROID) && !defined(DM_PLATFORM_IOS) && !defined(DM_PLATFORM_HTML5)

#include "sse_private.h"

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
    dmSnPrintf(error, error_size, "sse is not supported on this platform");
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

