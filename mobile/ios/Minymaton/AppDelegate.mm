#import "AppDelegate.h"

#import <React/RCTBundleURLProvider.h>

@implementation AppDelegate

- (BOOL)application:(UIApplication *)application didFinishLaunchingWithOptions:(NSDictionary *)launchOptions
{
  self.moduleName = @"Minymaton";
  // You can add your custom initial props in the dictionary below.
  // They will be passed down to the ViewController used by React Native.
  self.initialProps = @{};

  return [super application:application didFinishLaunchingWithOptions:launchOptions];
}

// Apps built with the iOS 27 SDK must adopt the scene life cycle; without
// a scene the runtime terminates the app at scene creation. The window is
// created by RCTAppDelegate at launch and attached to the scene here.
- (UISceneConfiguration *)application:(UIApplication *)application
    configurationForConnectingSceneSession:(UISceneSession *)connectingSceneSession
                                   options:(UISceneConnectionOptions *)options
{
  UISceneConfiguration *configuration =
      [[UISceneConfiguration alloc] initWithName:@"Default Configuration" sessionRole:connectingSceneSession.role];
  configuration.delegateClass = [self class];
  return configuration;
}

- (void)scene:(UIScene *)scene
    willConnectToSession:(UISceneSession *)session
                 options:(UISceneConnectionOptions *)connectionOptions
{
  if ([scene isKindOfClass:[UIWindowScene class]]) {
    // The scene delegate is a separate instance from the application
    // delegate; the window lives on the application delegate.
    AppDelegate *appDelegate = (AppDelegate *)UIApplication.sharedApplication.delegate;
    appDelegate.window.windowScene = (UIWindowScene *)scene;
    [appDelegate.window makeKeyAndVisible];
  }
}

- (NSURL *)sourceURLForBridge:(RCTBridge *)bridge
{
  return [self bundleURL];
}

- (NSURL *)bundleURL
{
  // The Detox debug build bundles the JS into the app (FORCE_BUNDLING);
  // prefer that bundle when present so no Metro server is needed, and fall
  // back to Metro for ordinary debug builds that do not bundle.
  NSURL *bundled = [[NSBundle mainBundle] URLForResource:@"main" withExtension:@"jsbundle"];
  if (bundled != nil) {
    return bundled;
  }
#if DEBUG
  return [[RCTBundleURLProvider sharedSettings] jsBundleURLForBundleRoot:@"index"];
#else
  return bundled;
#endif
}

@end
