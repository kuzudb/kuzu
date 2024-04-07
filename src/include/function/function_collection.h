#pragma once

#include "catalog/catalog_entry/catalog_entry_type.h"
#include "function.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace function {

using get_function_set_fun = std::function<function_set()>;

struct FunctionCollection {
    get_function_set_fun getFunctionSetFunc;

    const char* name;

    CatalogEntryType catalogEntryType;

    static FunctionCollection* getFunctions();
};

} // namespace function
} // namespace kuzu
