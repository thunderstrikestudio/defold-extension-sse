#ifndef EXTENSION_SSE_PARSER_H
#define EXTENSION_SSE_PARSER_H

#include <stddef.h>
#include <string>

struct SSEParsedEvent
{
    const char* m_Event;
    const char* m_Data;
    // The id: value of this event's own block, or "" if the block had none.
    const char* m_Id;
    // The parser's persistent last-event-id buffer: the most recent id: value
    // seen on the stream (id-only blocks included, may be "" for a spec-legal
    // reset), or NULL when no id: line has been seen since Reset. Adapters
    // commit this as the reconnect resume position once the event is
    // successfully delivered.
    const char* m_LastEventId;
};

typedef void (*SSEParserEventFn)(void* context, const SSEParsedEvent* event);
typedef void (*SSEParserRetryFn)(void* context, int retry_ms);
typedef void (*SSEParserIdFn)(void* context, const char* id);
typedef void (*SSEParserErrorFn)(void* context, const char* message);

struct SSEParserCallbacks
{
    SSEParserEventFn m_OnEvent;
    SSEParserRetryFn m_OnRetry;
    SSEParserIdFn m_OnId;
    SSEParserErrorFn m_OnError;

    SSEParserCallbacks()
    {
        m_OnEvent = 0;
        m_OnRetry = 0;
        m_OnId = 0;
        m_OnError = 0;
    }
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
    bool m_HasIdEver;
    bool m_FirstLine;
    bool m_DiscardingLine;
    bool m_EventPoisoned;
};

#endif
