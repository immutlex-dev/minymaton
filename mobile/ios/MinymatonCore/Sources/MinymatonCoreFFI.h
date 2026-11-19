#ifndef MinymatonCoreFFI_h
#define MinymatonCoreFFI_h

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*MinymatonEventCallback)(void *context, const char *eventJson);

void *minymaton_open(const char *dataDir, MinymatonEventCallback callback, void *context);
char *minymaton_request(void *handle, const char *commandJson);
void minymaton_free_string(char *value);
void minymaton_close(void *handle);

#ifdef __cplusplus
}
#endif

#endif /* MinymatonCoreFFI_h */
