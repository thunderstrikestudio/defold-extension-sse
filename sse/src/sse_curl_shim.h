#ifndef EXTENSION_SSE_CURL_SHIM_H
#define EXTENSION_SSE_CURL_SHIM_H

#if defined(__has_include)
#if __has_include(<curl/curl.h>)
#include <curl/curl.h>
#define SSE_HAS_SYSTEM_CURL_HEADER 1
#endif
#endif

#ifndef SSE_HAS_SYSTEM_CURL_HEADER

#include <stddef.h>

typedef void CURL;
typedef int CURLcode;
typedef int CURLoption;
typedef int CURLINFO;
typedef long long curl_off_t;

struct curl_slist
{
    char* data;
    curl_slist* next;
};

#define CURLE_OK 0
#define CURL_ERROR_SIZE 256
#define CURL_GLOBAL_DEFAULT 3

#define CURLOPTTYPE_LONG 0
#define CURLOPTTYPE_OBJECTPOINT 10000
#define CURLOPTTYPE_STRINGPOINT CURLOPTTYPE_OBJECTPOINT
#define CURLOPTTYPE_SLISTPOINT CURLOPTTYPE_OBJECTPOINT
#define CURLOPTTYPE_CBPOINT CURLOPTTYPE_OBJECTPOINT
#define CURLOPTTYPE_FUNCTIONPOINT 20000

#define CURLOPT_WRITEDATA (CURLOPTTYPE_CBPOINT + 1)
#define CURLOPT_URL (CURLOPTTYPE_STRINGPOINT + 2)
#define CURLOPT_ERRORBUFFER (CURLOPTTYPE_OBJECTPOINT + 10)
#define CURLOPT_WRITEFUNCTION (CURLOPTTYPE_FUNCTIONPOINT + 11)
#define CURLOPT_USERAGENT (CURLOPTTYPE_STRINGPOINT + 18)
#define CURLOPT_HTTPHEADER (CURLOPTTYPE_SLISTPOINT + 23)
#define CURLOPT_HEADERDATA (CURLOPTTYPE_CBPOINT + 29)
#define CURLOPT_NOPROGRESS (CURLOPTTYPE_LONG + 43)
#define CURLOPT_FOLLOWLOCATION (CURLOPTTYPE_LONG + 52)
#define CURLOPT_XFERINFODATA (CURLOPTTYPE_CBPOINT + 57)
#define CURLOPT_HEADERFUNCTION (CURLOPTTYPE_FUNCTIONPOINT + 79)
#define CURLOPT_NOSIGNAL (CURLOPTTYPE_LONG + 99)
#define CURLOPT_CONNECTTIMEOUT_MS (CURLOPTTYPE_LONG + 156)
#define CURLOPT_TCP_KEEPALIVE (CURLOPTTYPE_LONG + 213)
#define CURLOPT_XFERINFOFUNCTION (CURLOPTTYPE_FUNCTIONPOINT + 219)

#define CURLINFO_LONG 0x200000
#define CURLINFO_RESPONSE_CODE (CURLINFO_LONG + 2)

extern "C"
{
    CURLcode curl_global_init(long flags);
    void curl_global_cleanup(void);
    CURL* curl_easy_init(void);
    CURLcode curl_easy_setopt(CURL* curl, CURLoption option, ...);
    CURLcode curl_easy_perform(CURL* curl);
    CURLcode curl_easy_getinfo(CURL* curl, CURLINFO info, ...);
    void curl_easy_cleanup(CURL* curl);
    const char* curl_easy_strerror(CURLcode code);
    curl_slist* curl_slist_append(curl_slist* list, const char* string);
    void curl_slist_free_all(curl_slist* list);
}

#endif

#endif
