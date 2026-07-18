#if defined(DM_PLATFORM_OSX) || defined(DM_PLATFORM_LINUX) || defined(DM_PLATFORM_WINDOWS) || defined(SSE_PARSER_TEST)

#include "sse_parser.h"

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>

// Cap on the pending line buffer and on the accumulated event data. A stream
// that exceeds these is misbehaving (or hostile); the offending line/event is
// dropped with an error callback instead of growing memory without bound.
static const size_t SSE_PARSER_MAX_BUFFER = 1024u * 1024u;

// Server-provided retry values are clamped to a sane reconnect range.
static const long SSE_PARSER_RETRY_MIN_MS = 1000;
static const long SSE_PARSER_RETRY_MAX_MS = 300000;

SSEParser::SSEParser()
{
    Reset();
}

void SSEParser::Reset()
{
    m_Line.clear();
    m_Data.clear();
    m_Event.clear();
    m_Id.clear();
    m_HasId = false;
    m_FirstLine = true;
    m_DiscardingLine = false;
    m_EventPoisoned = false;
}

void SSEParser::Feed(const char* bytes, size_t size, const SSEParserCallbacks* callbacks, void* context)
{
    for (size_t i = 0; i < size; ++i)
    {
        const char c = bytes[i];
        if (c == '\n')
        {
            if (m_DiscardingLine)
            {
                m_DiscardingLine = false;
                m_Line.clear();
            }
            else
            {
                ProcessLine(m_Line, callbacks, context);
                m_Line.clear();
            }
        }
        else if (m_DiscardingLine)
        {
            // Dropping the remainder of an oversized line.
        }
        else
        {
            m_Line.push_back(c);
            if (m_Line.size() > SSE_PARSER_MAX_BUFFER)
            {
                if (callbacks && callbacks->m_OnError)
                {
                    callbacks->m_OnError(context, "SSE line exceeded maximum buffer size");
                }
                m_Line.clear();
                m_DiscardingLine = true;
                m_EventPoisoned = true;
            }
        }
    }
}

void SSEParser::ProcessLine(std::string line, const SSEParserCallbacks* callbacks, void* context)
{
    if (m_FirstLine)
    {
        m_FirstLine = false;
        if (line.size() >= 3
            && (unsigned char)line[0] == 0xEF
            && (unsigned char)line[1] == 0xBB
            && (unsigned char)line[2] == 0xBF)
        {
            line.erase(0, 3);
        }
    }

    if (!line.empty() && line[line.size() - 1] == '\r')
    {
        line.erase(line.size() - 1);
    }

    if (line.empty())
    {
        Dispatch(callbacks, context);
        return;
    }

    if (m_EventPoisoned)
    {
        // The current event lost data to a buffer cap; ignore its remaining
        // field lines until the blank line ends it.
        return;
    }

    if (line[0] == ':')
    {
        return;
    }

    const size_t colon = line.find(':');
    std::string field;
    std::string value;

    if (colon == std::string::npos)
    {
        field = line;
    }
    else
    {
        field = line.substr(0, colon);
        value = line.substr(colon + 1);
        if (!value.empty() && value[0] == ' ')
        {
            value.erase(0, 1);
        }
    }

    if (field == "data")
    {
        if (m_Data.size() + value.size() + 1 > SSE_PARSER_MAX_BUFFER)
        {
            if (callbacks && callbacks->m_OnError)
            {
                callbacks->m_OnError(context, "SSE event data exceeded maximum buffer size");
            }
            m_Data.clear();
            m_EventPoisoned = true;
            return;
        }
        m_Data += value;
        m_Data += '\n';
    }
    else if (field == "event")
    {
        m_Event = value;
    }
    else if (field == "id")
    {
        m_Id = value;
        m_HasId = true;
        if (callbacks && callbacks->m_OnId)
        {
            callbacks->m_OnId(context, m_Id.c_str());
        }
    }
    else if (field == "retry")
    {
        bool valid = !value.empty();
        for (size_t i = 0; i < value.size(); ++i)
        {
            if (!isdigit((unsigned char)value[i]))
            {
                valid = false;
                break;
            }
        }

        if (valid && callbacks && callbacks->m_OnRetry)
        {
            errno = 0;
            long parsed = strtol(value.c_str(), 0, 10);
            if (errno == ERANGE || parsed > SSE_PARSER_RETRY_MAX_MS)
            {
                parsed = SSE_PARSER_RETRY_MAX_MS;
            }
            if (parsed < SSE_PARSER_RETRY_MIN_MS)
            {
                parsed = SSE_PARSER_RETRY_MIN_MS;
            }
            callbacks->m_OnRetry(context, (int)parsed);
        }
    }
}

void SSEParser::Dispatch(const SSEParserCallbacks* callbacks, void* context)
{
    if (m_EventPoisoned)
    {
        m_EventPoisoned = false;
        m_Data.clear();
        m_Event.clear();
        m_Id.clear();
        m_HasId = false;
        return;
    }

    if (!m_Data.empty())
    {
        if (m_Data[m_Data.size() - 1] == '\n')
        {
            m_Data.erase(m_Data.size() - 1);
        }

        SSEParsedEvent event;
        event.m_Event = m_Event.empty() ? "message" : m_Event.c_str();
        event.m_Data = m_Data.c_str();
        event.m_Id = m_HasId ? m_Id.c_str() : "";

        if (callbacks && callbacks->m_OnEvent)
        {
            callbacks->m_OnEvent(context, &event);
        }
    }

    m_Data.clear();
    m_Event.clear();
    m_Id.clear();
    m_HasId = false;
}

#endif
