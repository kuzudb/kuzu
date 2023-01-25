#include <napi.h>
#include "njsdatabase.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  return NjsDatabase::Init(env, exports);
}

NODE_API_MODULE(NODE_GYP_MODULE_NAME, InitAll)
