#include "node_connection.h"
#include "node_database.h"
#include <napi.h>

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  NodeDatabase::Init(env, exports);
  NodeConnection::Init(env, exports);
  return exports;
}

NODE_API_MODULE(addon, InitAll)
