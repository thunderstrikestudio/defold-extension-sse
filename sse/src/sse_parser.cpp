#if defined(DM_PLATFORM_OSX) || defined(DM_PLATFORM_LINUX) || defined(DM_PLATFORM_WINDOWS) || defined(SSE_PARSER_TEST)

#include "sse_parser.h"

#include <ctype.h>
#include <stdlib.h>

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
}

void SSEParser::Feed(const char* bytes, size_t size, const SSEParserCallbacks* callbacks, void* context)
{
    for (size_t i = 0; i < size; ++i)
    {
        const char c = bytes[i];
        if (c == '\n')
        {
            ProcessLine(m_Line, callbacks, context);
            m_Line.clear();
        }
        else
        {
            m_Line.push_back(c);
        }
    }
}

void SSEParser::ProcessLine(std::string line, const SSEParserCallbacks* callbacks, void* context)
{
    if (!line.empty() && line[line.size() - 1] == '\r')
    {
        line.erase(line.size() - 1);
    }

    if (line.empty())
    {
        Dispatch(callbacks, context);
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
            callbacks->m_OnRetry(context, atoi(value.c_str()));
        }
    }
}

void SSEParser::Dispatch(const SSEParserCallbacks* callbacks, void* context)
{
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
