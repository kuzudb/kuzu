#pragma once

#include <napi.h>
#include "main/kuzu.h"

class Util {
    public:
        static Napi::Value ConvertToNapiObject(const kuzu::common::Value& value, Napi::Env env);
};
