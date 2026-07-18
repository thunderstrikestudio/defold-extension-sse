#if defined(DM_PLATFORM_ANDROID)

#include "sse_private.h"

#include <dmsdk/dlib/android.h>

struct SSEAndroidClient
{
    jobject m_Object;
};

static jclass g_SSEClientClass = 0;
static jmethodID g_SSEClientCtor = 0;
static jmethodID g_SSEClientConnect = 0;
static jmethodID g_SSEClientDisconnect = 0;

static bool SSEAndroid_LoadClass(JNIEnv* env)
{
    if (g_SSEClientClass)
    {
        return true;
    }

    jclass local_class = dmAndroid::LoadClass(env, "com.projecttower.sse.SseClient");
    if (!local_class)
    {
        dmLogError("Failed to load com.projecttower.sse.SseClient");
        return false;
    }

    g_SSEClientClass = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);

    g_SSEClientCtor = env->GetMethodID(g_SSEClientClass, "<init>", "(JLjava/lang/String;[Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;ZI)V");
    g_SSEClientConnect = env->GetMethodID(g_SSEClientClass, "connect", "()V");
    g_SSEClientDisconnect = env->GetMethodID(g_SSEClientClass, "disconnect", "()V");

    if (!g_SSEClientCtor || !g_SSEClientConnect || !g_SSEClientDisconnect)
    {
        dmLogError("Failed to resolve SseClient JNI methods");
        return false;
    }

    return true;
}

static jobjectArray SSEAndroid_NewStringArray(JNIEnv* env, uint32_t size)
{
    jclass string_class = env->FindClass("java/lang/String");
    jobjectArray array = env->NewObjectArray(size, string_class, 0);
    env->DeleteLocalRef(string_class);
    return array;
}

static void SSEAndroid_SetStringArrayItem(JNIEnv* env, jobjectArray array, uint32_t index, const char* value)
{
    jstring string_value = env->NewStringUTF(value ? value : "");
    env->SetObjectArrayElement(array, index, string_value);
    env->DeleteLocalRef(string_value);
}

extern "C"
{
    JNIEXPORT void JNICALL Java_com_projecttower_sse_SseClient_nativeOnOpen(JNIEnv* env, jclass, jlong handle, jint status)
    {
        SSE_EnqueueOpen((int32_t)handle, (int32_t)status);
    }

    JNIEXPORT void JNICALL Java_com_projecttower_sse_SseClient_nativeOnMessage(JNIEnv* env, jclass, jlong handle, jstring event_name, jstring data, jstring id)
    {
        const char* event_chars = event_name ? env->GetStringUTFChars(event_name, 0) : 0;
        const char* data_chars = data ? env->GetStringUTFChars(data, 0) : 0;
        const char* id_chars = id ? env->GetStringUTFChars(id, 0) : 0;

        SSE_EnqueueMessage((int32_t)handle, event_chars, data_chars, id_chars);

        if (id_chars)
        {
            SSE_SetLastEventId((int32_t)handle, id_chars);
            env->ReleaseStringUTFChars(id, id_chars);
        }
        if (data_chars)
        {
            env->ReleaseStringUTFChars(data, data_chars);
        }
        if (event_chars)
        {
            env->ReleaseStringUTFChars(event_name, event_chars);
        }
    }

    JNIEXPORT void JNICALL Java_com_projecttower_sse_SseClient_nativeOnError(JNIEnv* env, jclass, jlong handle, jstring error, jint status, jboolean reconnecting, jint retry_ms)
    {
        const char* error_chars = error ? env->GetStringUTFChars(error, 0) : "SSE error";
        SSE_EnqueueError((int32_t)handle, error_chars, (int32_t)status, reconnecting == JNI_TRUE, (int32_t)retry_ms);
        if (error)
        {
            env->ReleaseStringUTFChars(error, error_chars);
        }
    }

    JNIEXPORT void JNICALL Java_com_projecttower_sse_SseClient_nativeOnClosed(JNIEnv* env, jclass, jlong handle)
    {
        SSE_EnqueueClosed((int32_t)handle);
    }

    JNIEXPORT void JNICALL Java_com_projecttower_sse_SseClient_nativeOnRetry(JNIEnv* env, jclass, jlong handle, jint retry_ms)
    {
        (void)env;
        (void)handle;
        (void)retry_ms;
    }

    JNIEXPORT void JNICALL Java_com_projecttower_sse_SseClient_nativeOnLastEventId(JNIEnv* env, jclass, jlong handle, jstring id)
    {
        const char* id_chars = id ? env->GetStringUTFChars(id, 0) : "";
        SSE_SetLastEventId((int32_t)handle, id_chars);
        if (id)
        {
            env->ReleaseStringUTFChars(id, id_chars);
        }
    }
}

bool SSE_Platform_Initialize()
{
    dmAndroid::ThreadAttacher thread_attacher;
    return SSEAndroid_LoadClass(thread_attacher.GetEnv());
}

void SSE_Platform_Finalize()
{
    if (g_SSEClientClass)
    {
        dmAndroid::ThreadAttacher thread_attacher;
        JNIEnv* env = thread_attacher.GetEnv();
        env->DeleteGlobalRef(g_SSEClientClass);
    }

    g_SSEClientClass = 0;
    g_SSEClientCtor = 0;
    g_SSEClientConnect = 0;
    g_SSEClientDisconnect = 0;
}

bool SSE_Platform_IsSupported()
{
    return g_SSEClientClass != 0;
}

bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size)
{
    dmAndroid::ThreadAttacher thread_attacher;
    JNIEnv* env = thread_attacher.GetEnv();

    if (!SSEAndroid_LoadClass(env))
    {
        dmSnPrintf(error, error_size, "failed to load Android SSE Java adapter");
        return false;
    }

    jobjectArray keys = SSEAndroid_NewStringArray(env, connection->m_Headers.Size());
    jobjectArray values = SSEAndroid_NewStringArray(env, connection->m_Headers.Size());

    for (uint32_t i = 0; i < connection->m_Headers.Size(); ++i)
    {
        SSEAndroid_SetStringArrayItem(env, keys, i, connection->m_Headers[i].m_Name);
        SSEAndroid_SetStringArrayItem(env, values, i, connection->m_Headers[i].m_Value);
    }

    jstring url = env->NewStringUTF(connection->m_Url ? connection->m_Url : "");
    jstring last_event_id = env->NewStringUTF(connection->m_LastEventId ? connection->m_LastEventId : "");
    jobject local_object = env->NewObject(g_SSEClientClass, g_SSEClientCtor, (jlong)connection->m_Handle, url, keys, values, last_event_id, connection->m_Reconnect ? JNI_TRUE : JNI_FALSE, (jint)connection->m_RetryMS);

    env->DeleteLocalRef(url);
    env->DeleteLocalRef(last_event_id);
    env->DeleteLocalRef(keys);
    env->DeleteLocalRef(values);

    if (!local_object)
    {
        dmSnPrintf(error, error_size, "failed to create Android SSE client");
        return false;
    }

    SSEAndroidClient* android_client = new SSEAndroidClient;
    android_client->m_Object = env->NewGlobalRef(local_object);
    env->DeleteLocalRef(local_object);

    connection->m_PlatformData = android_client;
    env->CallVoidMethod(android_client->m_Object, g_SSEClientConnect);
    return true;
}

void SSE_Platform_Disconnect(SSEConnection* connection)
{
    SSEAndroidClient* android_client = (SSEAndroidClient*)connection->m_PlatformData;
    if (!android_client)
    {
        return;
    }

    dmAndroid::ThreadAttacher thread_attacher;
    JNIEnv* env = thread_attacher.GetEnv();
    env->CallVoidMethod(android_client->m_Object, g_SSEClientDisconnect);
    env->DeleteGlobalRef(android_client->m_Object);
    delete android_client;
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

