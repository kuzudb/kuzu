#include "node_connection.h"
#include "node_database.h"
#include <napi.h>

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  NodeDatabase::Init(env, exports);
  return NodeConnection::Init(env, exports);
}

NODE_API_MODULE(NODE_GYP_MODULE_NAME, InitAll)
