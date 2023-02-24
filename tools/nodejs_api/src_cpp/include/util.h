#pragma once

#include <ctime>
#include <iostream>
#include <chrono>

#include <napi.h>
#include "main/kuzu.h"

class Util {
    public:
        static Napi::Value ConvertToNapiObject(const kuzu::common::Value& value, Napi::Env env);
        static unsigned long GetEpochFromDate(int32_t year, int32_t month, int32_t day);
};
