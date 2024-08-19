#pragma once

#include <string>

#include "common/types/types.h"
#include "function/function.h"
#include "pybind_include.h"

using kuzu::common::LogicalTypeID;
using kuzu::function::function_set;

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main
} // namespace kuzu

class PyUDF {

public:
    static function_set toFunctionSet(const std::string& name, const py::function& udf,
        const py::list& paramTypes, const std::string& resultType, bool defaultNull,
        bool catchExceptions, kuzu::main::ClientContext* context);
};
