#if defined(DM_PLATFORM_IOS)

#include "sse_private.h"

#import <dispatch/dispatch.h>
#import <Foundation/Foundation.h>

// Reconnect backoff: the retry base is clamped to a sane range and grows
// exponentially (capped) with +-20% jitter so clients do not stampede.
#define SSE_IOS_RETRY_MIN_MS 1000
#define SSE_IOS_RETRY_MAX_MS 300000
#define SSE_IOS_MIN_DELAY_MS 500
#define SSE_IOS_MAX_BACKOFF_SHIFT 8
// Bound on the time between starting a request and receiving response
// headers; an established stream may idle indefinitely (no request timeout).
#define SSE_IOS_FIRST_RESPONSE_TIMEOUT_S 30
// Cap on the pending line buffer and on the accumulated event data.
#define SSE_IOS_MAX_BUFFER (1024 * 1024)

static int32_t SseIOS_ComputeRetryDelayMs(int32_t retry_ms, int32_t failures)
{
    int64_t base = retry_ms;
    if (base < SSE_IOS_RETRY_MIN_MS)
    {
        base = SSE_IOS_RETRY_MIN_MS;
    }
    if (base > SSE_IOS_RETRY_MAX_MS)
    {
        base = SSE_IOS_RETRY_MAX_MS;
    }

    int32_t shift = failures > 1 ? failures - 1 : 0;
    if (shift > SSE_IOS_MAX_BACKOFF_SHIFT)
    {
        shift = SSE_IOS_MAX_BACKOFF_SHIFT;
    }

    int64_t delay = base << shift;
    if (delay > SSE_IOS_RETRY_MAX_MS)
    {
        delay = SSE_IOS_RETRY_MAX_MS;
    }

    // +-20% jitter in 0.1% steps.
    const int64_t jitter = (int64_t)arc4random_uniform(401) - 200;
    delay += (delay * jitter) / 1000;
    if (delay < SSE_IOS_MIN_DELAY_MS)
    {
        delay = SSE_IOS_MIN_DELAY_MS;
    }

    return (int32_t)delay;
}

@interface SseIOSParser : NSObject
@property(atomic, assign) int32_t handle;
@property(atomic, retain) NSMutableData* lineBuffer;
@property(atomic, retain) NSMutableString* dataBuffer;
@property(atomic, retain) NSString* eventName;
@property(atomic, retain) NSString* eventId;
// Persistent last-event-id buffer: updated by every id: line (id-only blocks
// and empty spec-legal resets included), nil until the stream sends one.
// Never cleared by resetEvent so an id-only checkpoint is not lost.
@property(atomic, retain) NSString* pendingLastEventId;
// Rollback point: the buffer state at the end of the last completed block,
// restored when a block is poisoned so a dropped block's id is never
// committed as the resume position.
@property(atomic, retain) NSString* pendingAtBlockStart;
// The committed resume position: pendingLastEventId as of the most recently
// delivered event. hasCommittedLastEventId distinguishes "" (reset) from
// "never committed".
@property(atomic, retain) NSString* storedLastEventId;
@property(atomic, assign) BOOL hasCommittedLastEventId;
// Set once an event is dropped (queue full). From then on the resume
// position is frozen for the rest of this stream attempt, so a later
// accepted id can never commit past the undelivered event; the next
// reconnect then replays from before the drop.
@property(atomic, assign) BOOL dropObserved;
@property(atomic, assign) BOOL hasId;
@property(atomic, assign) BOOL reconnecting;
@property(atomic, assign) BOOL firstLine;
@property(atomic, assign) BOOL discardingLine;
@property(atomic, assign) BOOL eventPoisoned;
@property(atomic, assign) int32_t retryMs;
- (id)initWithHandle:(int32_t)handle retryMs:(int32_t)retryMs reconnecting:(BOOL)reconnecting;
- (void)feedData:(NSData*)data;
- (void)resetEvent;
@end

@implementation SseIOSParser

@synthesize handle;
@synthesize lineBuffer;
@synthesize dataBuffer;
@synthesize eventName;
@synthesize eventId;
@synthesize pendingLastEventId;
@synthesize pendingAtBlockStart;
@synthesize storedLastEventId;
@synthesize hasCommittedLastEventId;
@synthesize dropObserved;
@synthesize hasId;
@synthesize reconnecting;
@synthesize firstLine;
@synthesize discardingLine;
@synthesize eventPoisoned;
@synthesize retryMs;

- (id)initWithHandle:(int32_t)inputHandle retryMs:(int32_t)inputRetryMs reconnecting:(BOOL)inputReconnecting
{
    self = [super init];
    if (self)
    {
        self.handle = inputHandle;
        self.retryMs = inputRetryMs;
        self.reconnecting = inputReconnecting;
        self.lineBuffer = [NSMutableData data];
        self.dataBuffer = [NSMutableString string];
        self.eventName = @"";
        self.eventId = @"";
        self.pendingLastEventId = nil;
        self.pendingAtBlockStart = nil;
        self.storedLastEventId = @"";
        self.hasCommittedLastEventId = NO;
        self.dropObserved = NO;
        self.hasId = NO;
        self.firstLine = YES;
        self.discardingLine = NO;
        self.eventPoisoned = NO;
    }
    return self;
}

- (void)dealloc
{
    self.lineBuffer = nil;
    self.dataBuffer = nil;
    self.eventName = nil;
    self.eventId = nil;
    self.pendingLastEventId = nil;
    self.pendingAtBlockStart = nil;
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

- (void)poisonEvent:(const char*)message
{
    // The current event lost data to a buffer cap; report it, reset the
    // accumulated state and suppress the event until its blank line, so a
    // truncated payload is never delivered as a complete event. Any id: the
    // poisoned block already parsed is rolled back so a dropped block can
    // never advance the committed resume position.
    SSE_EnqueueError(self.handle, message, 0, self.reconnecting, self.retryMs);
    self.pendingLastEventId = self.pendingAtBlockStart;
    [self resetEvent];
    self.eventPoisoned = YES;
}

- (void)processLineData
{
    NSUInteger length = [self.lineBuffer length];
    const unsigned char* bytes = (const unsigned char*)[self.lineBuffer bytes];

    if (self.firstLine)
    {
        self.firstLine = NO;
        if (length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
        {
            bytes += 3;
            length -= 3;
        }
    }

    if (length > 0 && bytes[length - 1] == '\r')
    {
        length -= 1;
    }

    NSString* line = [[[NSString alloc] initWithBytes:bytes length:length encoding:NSUTF8StringEncoding] autorelease];
    [self.lineBuffer setLength:0];

    if (!line)
    {
        SSE_EnqueueError(self.handle, "SSE stream contained invalid UTF-8", 0, self.reconnecting, self.retryMs);
        return;
    }

    if ([line length] == 0)
    {
        [self dispatchEvent];
        return;
    }

    if (self.eventPoisoned)
    {
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
        // + 1 accounts for the appended newline, so a flood of empty data:
        // lines still trips the cap.
        if ([self.dataBuffer length] + [value length] + 1 > SSE_IOS_MAX_BUFFER)
        {
            [self poisonEvent:"SSE event data exceeded maximum buffer size"];
            return;
        }
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
        self.hasId = YES;
        self.pendingLastEventId = value;
    }
    else if ([field isEqualToString:@"retry"] && [self isInteger:value])
    {
        long long parsed = [value longLongValue];
        if (parsed < SSE_IOS_RETRY_MIN_MS)
        {
            parsed = SSE_IOS_RETRY_MIN_MS;
        }
        if (parsed > SSE_IOS_RETRY_MAX_MS)
        {
            parsed = SSE_IOS_RETRY_MAX_MS;
        }
        self.retryMs = (int32_t)parsed;
    }
}

- (void)dispatchEvent
{
    if (self.eventPoisoned)
    {
        self.eventPoisoned = NO;
        [self resetEvent];
        return;
    }

    if ([self.dataBuffer length] == 0)
    {
        if (self.hasId && !self.dropObserved && self.pendingLastEventId != nil)
        {
            // An id-only checkpoint block carries no payload that could be
            // dropped; commit the resume position now so it survives a
            // connection drop before the next data event.
            self.storedLastEventId = self.pendingLastEventId;
            self.hasCommittedLastEventId = YES;
            SSE_SetLastEventId(self.handle, [self.pendingLastEventId UTF8String]);
        }
        [self resetEvent];
        return;
    }

    [self.dataBuffer deleteCharactersInRange:NSMakeRange([self.dataBuffer length] - 1, 1)];
    NSString* name = [self.eventName length] == 0 ? @"message" : self.eventName;
    NSString* idValue = self.hasId ? self.eventId : @"";

    const bool enqueued = SSE_EnqueueMessage(self.handle, [name UTF8String], [self.dataBuffer UTF8String], [idValue UTF8String]);
    if (enqueued)
    {
        if (!self.dropObserved && self.pendingLastEventId != nil)
        {
            // Commit the persistent buffer only when the event was actually
            // delivered; otherwise a reconnect would skip the events dropped
            // on queue overflow. An empty value is a spec-legal reset and
            // clears the Last-Event-ID header.
            self.storedLastEventId = self.pendingLastEventId;
            self.hasCommittedLastEventId = YES;
            SSE_SetLastEventId(self.handle, [self.pendingLastEventId UTF8String]);
        }
    }
    else
    {
        // The event was dropped (queue full); roll back its id and freeze
        // the resume position for the rest of this attempt, so neither this
        // nor any later id can commit past an event the callback never
        // received. The next reconnect replays from before the drop.
        self.pendingLastEventId = self.pendingAtBlockStart;
        self.dropObserved = YES;
    }
    [self resetEvent];
}

- (void)resetEvent
{
    [self.dataBuffer setString:@""];
    self.eventName = @"";
    self.eventId = @"";
    self.hasId = NO;
    // The block is over; its id state becomes the rollback point for a
    // future poisoned block.
    self.pendingAtBlockStart = self.pendingLastEventId;
}

- (void)feedData:(NSData*)data
{
    const unsigned char* bytes = (const unsigned char*)[data bytes];
    NSUInteger length = [data length];

    for (NSUInteger i = 0; i < length; ++i)
    {
        if (bytes[i] == '\n')
        {
            if (self.discardingLine)
            {
                self.discardingLine = NO;
                [self.lineBuffer setLength:0];
            }
            else
            {
                [self processLineData];
            }
        }
        else if (self.discardingLine)
        {
            // Dropping the remainder of an oversized line.
        }
        else
        {
            [self.lineBuffer appendBytes:&bytes[i] length:1];
            if ([self.lineBuffer length] > SSE_IOS_MAX_BUFFER)
            {
                [self poisonEvent:"SSE line exceeded maximum buffer size"];
                [self.lineBuffer setLength:0];
                self.discardingLine = YES;
            }
        }
    }
}

@end

@interface SseIOSClient : NSObject<NSURLSessionDataDelegate>
@property(atomic, assign) int32_t handle;
@property(atomic, retain) NSString* url;
@property(atomic, retain) NSDictionary* headers;
@property(atomic, retain) NSString* lastEventId;
@property(atomic, retain) NSURLSession* session;
@property(atomic, retain) NSURLSessionDataTask* task;
@property(atomic, retain) SseIOSParser* parser;
@property(atomic, assign) BOOL reconnect;
@property(atomic, assign) BOOL stopped;
@property(atomic, assign) BOOL failedResponse;
@property(atomic, assign) BOOL receivedResponse;
@property(atomic, assign) BOOL firstResponseTimedOut;
@property(atomic, assign) int32_t retryMs;
@property(atomic, assign) int32_t status;
@property(atomic, assign) int32_t failedAttempts;
// Serial queue that owns all client state mutation: connect, disconnect,
// reconnect scheduling and every NSURLSession delegate callback run on it.
@property(atomic, assign) dispatch_queue_t queue;
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
@synthesize receivedResponse;
@synthesize firstResponseTimedOut;
@synthesize retryMs;
@synthesize status;
@synthesize failedAttempts;
@synthesize queue;

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
        self.failedAttempts = 0;
        self.queue = dispatch_queue_create("com.projecttower.sse.client", DISPATCH_QUEUE_SERIAL);
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
    if (self.queue)
    {
        dispatch_release(self.queue);
        self.queue = nil;
    }
    [super dealloc];
}

- (void)connect
{
    // stopped is NO from init; startRequest simply runs after any already
    // queued work on the serial queue.
    dispatch_async(self.queue, ^{
        [self startRequest];
    });
}

- (void)disconnect
{
    // Called from the engine main thread. The atomic stopped flag is set
    // immediately so in-flight callbacks and pending reconnect blocks bail,
    // and the actual teardown is dispatched asynchronously so the engine
    // never waits behind queued parser work (the block retains the client,
    // and the serial queue orders it after any pending delegate callbacks).
    self.stopped = YES;
    dispatch_async(self.queue, ^{
        [self.task cancel];
        [self.session invalidateAndCancel];
        self.task = nil;
        self.session = nil;
    });
}

// Runs on the serial queue.
- (void)startRequest
{
    if (self.stopped)
    {
        return;
    }

    self.failedResponse = NO;
    self.receivedResponse = NO;
    self.firstResponseTimedOut = NO;
    self.status = 0;
    self.parser = [[[SseIOSParser alloc] initWithHandle:self.handle retryMs:self.retryMs reconnecting:self.reconnect] autorelease];

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
    // No request/resource timeout: an SSE stream may legitimately idle for a
    // long time. Time-to-first-response is bounded by the watchdog below.
    config.timeoutIntervalForRequest = 0;
    config.timeoutIntervalForResource = 0;

    // Deliver all delegate callbacks on the client's serial queue so they
    // never race connect/disconnect/reconnect state changes.
    NSOperationQueue* delegateQueue = [[[NSOperationQueue alloc] init] autorelease];
    delegateQueue.maxConcurrentOperationCount = 1;
    delegateQueue.underlyingQueue = self.queue;

    self.session = [NSURLSession sessionWithConfiguration:config delegate:self delegateQueue:delegateQueue];
    self.task = [self.session dataTaskWithRequest:request];
    [self.task resume];

    NSURLSessionDataTask* watchedTask = self.task;
    SseIOSClient* retainedSelf = [self retain];
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, (int64_t)SSE_IOS_FIRST_RESPONSE_TIMEOUT_S * (int64_t)NSEC_PER_SEC), self.queue, ^{
        if (!retainedSelf.stopped && !retainedSelf.receivedResponse && retainedSelf.task == watchedTask)
        {
            retainedSelf.firstResponseTimedOut = YES;
            SSE_EnqueueError(retainedSelf.handle, "SSE timed out waiting for server response", 0, retainedSelf.reconnect, retainedSelf.retryMs);
            [watchedTask cancel];
        }
        [retainedSelf release];
    });
}

// Runs on the serial queue.
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

    self.failedAttempts = self.failedAttempts + 1;
    const int32_t delay_ms = SseIOS_ComputeRetryDelayMs(self.retryMs, self.failedAttempts);
    SseIOSClient* retainedSelf = [self retain];
    int64_t delay = (int64_t)delay_ms * (int64_t)NSEC_PER_MSEC;
    dispatch_after(dispatch_time(DISPATCH_TIME_NOW, delay), self.queue, ^{
        [retainedSelf startRequest];
        [retainedSelf release];
    });
}

- (void)URLSession:(NSURLSession*)session dataTask:(NSURLSessionDataTask*)dataTask didReceiveResponse:(NSURLResponse*)response completionHandler:(void (^)(NSURLSessionResponseDisposition disposition))completionHandler
{
    if (self.stopped || self.firstResponseTimedOut)
    {
        // The watchdog already reported this attempt as timed out (or the
        // user disconnected) and the task was cancelled; don't surface an
        // open or status error for it.
        completionHandler(NSURLSessionResponseCancel);
        return;
    }

    self.receivedResponse = YES;

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

    self.failedAttempts = 0;
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

    if (error && !self.failedResponse && !self.firstResponseTimedOut)
    {
        SSE_EnqueueError(self.handle, [[error localizedDescription] UTF8String], self.status, self.reconnect, self.retryMs);
    }
    self.firstResponseTimedOut = NO;

    self.retryMs = self.parser.retryMs;
    if (self.parser.hasCommittedLastEventId)
    {
        // May be @"" for a spec-legal reset, which suppresses the
        // Last-Event-ID header on the next attempt.
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
