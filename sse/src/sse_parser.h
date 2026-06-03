#ifndef EXTENSION_SSE_PARSER_H
#define EXTENSION_SSE_PARSER_H

#include <stddef.h>
#include <string>

struct SSEParsedEvent
{
    const char* m_Event;
    const char* m_Data;
    const char* m_Id;
};

typedef void (*SSEParserEventFn)(void* context, const SSEParsedEvent* event);
typedef void (*SSEParserRetryFn)(void* context, int retry_ms);
typedef void (*SSEParserIdFn)(void* context, const char* id);

struct SSEParserCallbacks
{
    SSEParserEventFn m_OnEvent;
    SSEParserRetryFn m_OnRetry;
    SSEParserIdFn m_OnId;
};

class SSEParser
{
public:
    SSEParser();

    void Reset();
    void Feed(const char* bytes, size_t size, const SSEParserCallbacks* callbacks, void* context);

private:
    void ProcessLine(std::string line, const SSEParserCallbacks* callbacks, void* context);
    void Dispatch(const SSEParserCallbacks* callbacks, void* context);

    std::string m_Line;
    std::string m_Data;
    std::string m_Event;
    std::string m_Id;
    bool m_HasId;
};

#endif

