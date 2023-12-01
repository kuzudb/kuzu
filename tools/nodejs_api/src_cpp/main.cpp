#include "include/node_connection.h"
#include "include/node_database.h"
#include "include/node_query_result.h"
#include <napi.h>

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
    NodeConnection::Init(env, exports);
    NodeDatabase::Init(env, exports);
    NodePreparedStatement::Init(env, exports);
    NodeQueryResult::Init(env, exports);
    return exports;
}

NODE_API_MODULE(addon, InitAll);
