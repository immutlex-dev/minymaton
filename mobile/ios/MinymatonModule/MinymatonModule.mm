#import "MinymatonModule.h"
#import <Security/Security.h>
// The bridge is a Swift (@objc) class in the MinymatonCore pod. Its
// generated Swift header is not importable from this ObjC++ file (C++
// modules are disabled for it), so the ObjC surface is declared here and
// resolved from the Swift module at link time.
@interface MinymatonCoreBridge : NSObject
@property (nonatomic, copy, nullable) void (^onEvent)(NSString *);
- (nullable instancetype)initWithDataDir:(NSString *)dataDir error:(NSError **)error;
- (void)request:(NSString *)command completion:(void (^)(NSString *_Nullable, NSError *_Nullable))completion;
- (void)close;
@end

static NSString *const kEventName = @"MinymatonEvent";
static NSString *const kService = @"dev.minymaton.bus-tokens";

@interface MinymatonModule () {
    BOOL _hasListeners;
}
@property (nonatomic, strong) MinymatonCoreBridge *core;
@property (nonatomic, strong) dispatch_queue_t worker;
@end

@implementation MinymatonModule

RCT_EXPORT_MODULE(MinymatonModule);

- (instancetype)init {
    if ((self = [super init])) {
        _worker = dispatch_queue_create("dev.minymaton.module", DISPATCH_QUEUE_SERIAL);
        NSString *dir = [NSSearchPathForDirectoriesInDomains(NSDocumentDirectory, NSUserDomainMask, YES).firstObject
            stringByAppendingPathComponent:@"minymaton"];
        NSError *error = nil;
        _core = [[MinymatonCoreBridge alloc] initWithDataDir:dir error:&error];
        [[NSURL fileURLWithPath:dir isDirectory:YES] setResourceValue:@YES forKey:NSURLIsExcludedFromBackupKey error:nil];
        __weak MinymatonModule *weakSelf = self;
        _core.onEvent = ^(NSString *event) {
            // Log the event kind only (never the payload), mirroring the
            // Android module so e2e/core_log.ts can await lifecycle lines.
            NSDictionary *parsed = [NSJSONSerialization JSONObjectWithData:[event dataUsingEncoding:NSUTF8StringEncoding] options:0 error:nil];
            NSLog(@"MinymatonCore: %@", parsed[@"event"] ?: @"unknown");
            MinymatonModule *strongSelf = weakSelf;
            if (strongSelf != nil && strongSelf->_hasListeners) {
                [strongSelf sendEventWithName:kEventName body:event];
            }
        };
    }
    return self;
}

- (NSArray<NSString *> *)supportedEvents {
    return @[kEventName];
}

+ (BOOL)requiresMainQueueSetup {
    return NO;
}

- (void)invalidate {
    [_core close];
    _core = nil;
    [super invalidate];
}

RCT_EXPORT_METHOD(request:(NSString *)command
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject) {
    [self.core request:command completion:^(NSString *response, NSError *error) {
        if (error != nil) {
            reject(@"core_failed", error.localizedDescription, error);
        } else {
            resolve(response);
        }
    }];
}

static NSMutableDictionary *tokenQuery(NSString *profileId) {
    return [@{
        (__bridge id)kSecClass: (__bridge id)kSecClassGenericPassword,
        (__bridge id)kSecAttrService: kService,
        (__bridge id)kSecAttrAccount: profileId,
    } mutableCopy];
}

RCT_EXPORT_METHOD(saveToken:(NSString *)profileId
                  token:(NSString *)token
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject) {
    dispatch_async(self.worker, ^{
        NSMutableDictionary *query = tokenQuery(profileId);
        SecItemDelete((__bridge CFDictionaryRef)query);
        query[(__bridge id)kSecValueData] = [token dataUsingEncoding:NSUTF8StringEncoding];
        query[(__bridge id)kSecAttrAccessible] = (__bridge id)kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly;
        OSStatus status = SecItemAdd((__bridge CFDictionaryRef)query, NULL);
        if (status == errSecSuccess) {
            resolve(nil);
        } else {
            reject(@"keystore_failed", [NSString stringWithFormat:@"SecItemAdd failed: %d", (int)status], nil);
        }
    });
}

RCT_EXPORT_METHOD(loadToken:(NSString *)profileId
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject) {
    dispatch_async(self.worker, ^{
        NSMutableDictionary *query = tokenQuery(profileId);
        query[(__bridge id)kSecReturnData] = @YES;
        query[(__bridge id)kSecMatchLimit] = (__bridge id)kSecMatchLimitOne;
        CFTypeRef result = NULL;
        OSStatus status = SecItemCopyMatching((__bridge CFDictionaryRef)query, &result);
        if (status == errSecItemNotFound) {
            resolve(nil);
        } else if (status == errSecSuccess && result != NULL) {
            NSString *token = [[NSString alloc] initWithData:(__bridge NSData *)result encoding:NSUTF8StringEncoding];
            CFRelease(result);
            resolve(token);
        } else {
            reject(@"keystore_failed", [NSString stringWithFormat:@"SecItemCopyMatching failed: %d", (int)status], nil);
        }
    });
}

RCT_EXPORT_METHOD(deleteToken:(NSString *)profileId
                  resolver:(RCTPromiseResolveBlock)resolve
                  rejecter:(RCTPromiseRejectBlock)reject) {
    dispatch_async(self.worker, ^{
        SecItemDelete((__bridge CFDictionaryRef)tokenQuery(profileId));
        resolve(nil);
    });
}

- (void)startObserving {
    _hasListeners = YES;
}

- (void)stopObserving {
    _hasListeners = NO;
}

@end
