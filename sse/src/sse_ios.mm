#if defined(DM_PLATFORM_IOS)

#include "sse_private.h"

#import <dispatch/dispatch.h>
#import <Foundation/Foundation.h>

@interface SseIOSParser : NSObject
@property(nonatomic, assign) int32_t handle;
@property(nonatomic, retain) NSMutableData* lineBuffer;
@property(nonatomic, retain) NSMutableString* dataBuffer;
@property(nonatomic, retain) NSString* eventName;
@property(nonatomic, retain) NSString* eventId;
@property(nonatomic, retain) NSString* storedLastEventId;
@property(nonatomic, assign) BOOL hasId;
@property(nonatomic, assign) int32_t retryMs;
- (id)initWithHandle:(int32_t)handle retryMs:(int32_t)retryMs;
- (void)feedData:(NSData*)data;
- (void)resetEvent;
@end

@implementation SseIOSParser

@synthesize handle;
@synthesize lineBuffer;
@synthesize dataBuffer;
@synthesize eventName;
@synthesize eventId;
@synthesize storedLastEventId;
@synthesize hasId;
@synthesize retryMs;

- (id)initWithHandle:(int32_t)inputHandle retryMs:(int32_t)inputRetryMs
{
    self = [super init];
    if (self)
    {
        self.handle = inputHandle;
        self.retryMs = inputRetryMs;
        self.lineBuffer = [NSMutableData data];
        self.dataBuffer = [NSMutableString string];
        self.eventName = @"";
        self.eventId = @"";
        self.storedLastEventId = @"";
        self.hasId = NO;
    }
    return self;
}

- (void)dealloc
{
    self.lineBuffer = nil;
    self.dataBuffer = nil;
    self.eventName = nil;
    self.eventId = nil;
    self.storedLastEventId = nil;
    [super dealloc];
}

- (BOOL)isInteger:(NSString*)value
{
    if (!value || [value length] == 0)
    {
        return NO;
    }

    NSCharacterSet* nonDigits = [[NSCharacterSet decimalDigitCharacterSet] invertedSet];
    return [value rangeOfCharacterFromSet:nonDigits].location == NSNotFound;
}

- (void)processLineData
{
    NSUInteger length = [self.lineBuffer length];
    const unsigned char* bytes = (const unsigned char*)[self.lineBuffer bytes];
    if (length > 0 && bytes[length - 1] == '\r')
    {
        length -= 1;
    }

    NSString* line = [[[NSString alloc] initWithBytes:bytes length:length encoding:NSUTF8StringEncoding] autorelease];
    [self.lineBuffer setLength:0];

    if (!line)
    {
        SSE_EnqueueError(self.handle, "SSE stream contained invalid UTF-8", 0, false, self.retryMs);
        return;
    }

    if ([line length] == 0)
    {
        [self dispatchEvent];
        return;
    }

    if ([line characterAtIndex:0] == ':')
    {
        return;
    }

    NSRange colon = [line rangeOfString:@":"];
    NSString* field = line;
    NSString* value = @"";

    if (colon.location != NSNotFound)
    {
        field = [line substringToIndex:colon.location];
        value = [line substringFromIndex:colon.location + 1];
        if ([value hasPrefix:@" "])
        {
            value = [value substringFromIndex:1];
        }
    }

    if ([field isEqualToString:@"data"])
    {
        [self.dataBuffer appendString:value];
        [self.dataBuffer appendString:@"\n"];
    }
    else if ([field isEqualToString:@"event"])
    {
        self.eventName = value;
    }
    else if ([field isEqualToString:@"id"])
    {
        self.eventId = value;
        self.storedLastEventId = value;
        self.hasId = YES;
        SSE_SetLastEventId(self.handle, [value UTF8String]);
    }
    else if ([field isEqualToString:@"retry"] && [self isInteger:value])
    {
        self.retryMs = [value intValue];
    }
}

- (void)dispatchEvent
{
    if ([self.dataBuffer length] == 0)
    {
        [self resetEvent];
        return;
    }

    [self.dataBuffer deleteCharactersInRange:NSMakeRange([self.dataBuffer length] - 1, 1)];
    NSString* name = [self.eventName length] == 0 ? @"message" : self.eventName;
    NSString* idValue = self.hasId ? self.eventId : @"";

    SSE_EnqueueMessage(self.handle, [name UTF8String], [self.dataBuffer UTF8String], [idValue UTF8String]);
    [self resetEvent];
}

- (void)resetEvent
{
    [self.dataBuffer setString:@""];
    self.eventName = @"";
    self.eventId = @"";
    self.hasId = NO;
}

- (void)feedData:(NSData*)data
{
    const unsigned char* bytes = (const unsigned char*)[data bytes];
    NSUInteger length = [data length];

    for (NSUInteger i = 0; i < length; ++i)
    {
        if (bytes[i] == '\n')
        {
            [self processLineData];
        }
        else
        {
            [self.lineBuffer appendBytes:&bytes[i] length:1];
        }
    }
}

@end

@interface SseIOSClient : NSObject<NSURLSessionDataDelegate>
@property(nonatomic, assign) int32_t handle;
@property(nonatomic, retain) NSString* url;
@property(nonatomic, retain) NSDictionary* headers;
@property(nonatomic, retain) NSString* lastEventId;
@property(nonatomic, retain) NSURLSession* session;
@property(nonatomic, retain) NSURLSessionDataTask* task;
@property(nonatomic, retain) SseIOSParser* parser;
@property(nonatomic, assign) BOOL reconnect;
@property(nonatomic, assign) BOOL stopped;
@property(nonatomic, assign) BOOL failedResponse;
@property(nonatomic, assign) int32_t retryMs;
@property(nonatomic, assign) int32_t status;
- (id)initWithConnection:(SSEConnection*)connection;
- (void)connect;
- (void)disconnect;
@end

@implementation SseIOSClient

@synthesize handle;
@synthesize url;
@synthesize headers;
@synthesize lastEventId;
@synthesize session;
@synthesize task;
@synthesize parser;
@synthesize reconnect;
@synthesize stopped;
@synthesize failedResponse;
@synthesize retryMs;
@synthesize status;

- (id)initWithConnection:(SSEConnection*)connection
{
    self = [super init];
    if (self)
    {
        self.handle = connection->m_Handle;
        self.url = [NSString stringWithUTF8String:connection->m_Url ? connection->m_Url : ""];
        self.lastEventId = [NSString stringWithUTF8String:connection->m_LastEventId ? connection->m_LastEventId : ""];
        self.reconnect = connection->m_Reconnect != 0;
        self.retryMs = connection->m_RetryMS;
        self.status = 0;
        NSMutableDictionary* headerDict = [NSMutableDictionary dictionary];
        for (uint32_t i = 0; i < connection->m_Headers.Size(); ++i)
        {
            NSString* name = [NSString stringWithUTF8String:connection->m_Headers[i].m_Name ? connection->m_Headers[i].m_Name : ""];
            NSString* value = [NSString stringWithUTF8String:connection->m_Headers[i].m_Value ? connection->m_Headers[i].m_Value : ""];
            if ([name length] > 0)
            {
                [headerDict setObject:value forKey:name];
            }
        }
        self.headers = headerDict;
    }
    return self;
}

- (void)dealloc
{
    self.url = nil;
    self.headers = nil;
    self.lastEventId = nil;
    self.session = nil;
    self.task = nil;
    self.parser = nil;
    [super dealloc];
}

- (void)connect
{
    self.stopped = NO;
    [self startRequest];
}

- (void)disconnect
{
    self.stopped = YES;
    [self.task cancel];
    [self.session invalidateAndCancel];
    self.task = nil;
    self.session = nil;
}

- (void)startRequest
{
    if (self.stopped)
    {
        return;
    }

    self.failedResponse = NO;
    self.status = 0;
    self.parser = [[[SseIOSParser alloc] initWithHandle:self.handle retryMs:self.retryMs] autorelease];

    NSURL* nsurl = [NSURL URLWithString:self.url];
    NSMutableURLRequest* request = [NSMutableURLRequest requestWithURL:nsurl];
    [request setHTTPMethod:@"GET"];
    [request setValue:@"text/event-stream" forHTTPHeaderField:@"Accept"];
    [request setValue:@"no-cache" forHTTPHeaderField:@"Cache-Control"];

    for (NSString* key in self.headers)
    {
        [request setValue:[self.headers objectForKey:key] forHTTPHeaderField:key];
    }

    if ([self.lastEventId length] > 0)
    {
        [request setValue:self.lastEventId forHTTPHeaderField:@"Last-Event-ID"];
    }

    NSURLSessionConfiguration* config = [NSURLSessionConfiguration defaultSessionConfiguration];
    config.timeoutIntervalForRequest = 0;
    config.timeoutIntervalForResource = 0;

    self.session = [NSURLSession sessionWithConfiguration:config delegate:self delegateQueue:nil];
    self.task = [self.session dataTaskWithRequest:request];
    [self.task resume];
}

- (void)scheduleReconnectOrClose
{
    if (self.stopped)
    {
        return;
    }

    if (!self.reconnect)
    {
        SSE_EnqueueClosed(self.handle);
        return;
    }

    SseIOSClient* retainedSelf = [self retain];
    int64_t delay = (int64_t)self.retryMs * (int64_t)NSEC_PER_MSEC;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, delay), dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        [retainedSelf startRequest];
        [retainedSelf release];
    });
}

- (void)URLSession:(NSURLSession*)session dataTask:(NSURLSessionDataTask*)dataTask didReceiveResponse:(NSURLResponse*)response completionHandler:(void (^)(NSURLSessionResponseDisposition disposition))completionHandler
{
    NSHTTPURLResponse* http = (NSHTTPURLResponse*)response;
    self.status = (int32_t)[http statusCode];

    if (self.status < 200 || self.status >= 300)
    {
        self.failedResponse = YES;
        NSString* message = [NSString stringWithFormat:@"SSE request failed with HTTP status %d", self.status];
        SSE_EnqueueError(self.handle, [message UTF8String], self.status, self.reconnect, self.retryMs);
        completionHandler(NSURLSessionResponseCancel);
        return;
    }

    SSE_EnqueueOpen(self.handle, self.status);
    completionHandler(NSURLSessionResponseAllow);
}

- (void)URLSession:(NSURLSession*)session dataTask:(NSURLSessionDataTask*)dataTask didReceiveData:(NSData*)data
{
    [self.parser feedData:data];
}

- (void)URLSession:(NSURLSession*)session task:(NSURLSessionTask*)task didCompleteWithError:(NSError*)error
{
    SSE_SetConnected(self.handle, false);

    if (self.stopped)
    {
        return;
    }

    if (error && !self.failedResponse)
    {
        SSE_EnqueueError(self.handle, [[error localizedDescription] UTF8String], self.status, self.reconnect, self.retryMs);
    }

    self.retryMs = self.parser.retryMs;
    if ([self.parser.storedLastEventId length] > 0)
    {
        self.lastEventId = self.parser.storedLastEventId;
    }

    [self.session finishTasksAndInvalidate];
    self.session = nil;
    self.task = nil;
    [self scheduleReconnectOrClose];
}

@end

bool SSE_Platform_Initialize()
{
    return true;
}

void SSE_Platform_Finalize()
{
}

bool SSE_Platform_IsSupported()
{
    return true;
}

bool SSE_Platform_Connect(SSEConnection* connection, char* error, uint32_t error_size)
{
    SseIOSClient* client = [[SseIOSClient alloc] initWithConnection:connection];
    if (!client)
    {
        dmSnPrintf(error, error_size, "failed to create iOS SSE client");
        return false;
    }

    connection->m_PlatformData = client;
    [client connect];
    return true;
}

void SSE_Platform_Disconnect(SSEConnection* connection)
{
    SseIOSClient* client = (SseIOSClient*)connection->m_PlatformData;
    if (!client)
    {
        return;
    }

    [client disconnect];
    [client release];
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
