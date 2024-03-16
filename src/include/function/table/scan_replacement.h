#pragma once

#include "function/table_functions.h"
#include "function/table/bind_input.h"

namespace kuzu {
namespace function {

struct ScanReplacementData {
    TableFunction func;
    TableFuncBindInput bindInput;
};

using scan_replace_func_t = std::function<std::unique_ptr<ScanReplacementData>(const std::string&)>;

struct ScanReplacement {
    ScanReplacement(scan_replace_func_t replaceFunc) : replaceFunc{replaceFunc} {}

    scan_replace_func_t replaceFunc;
};

}
}
