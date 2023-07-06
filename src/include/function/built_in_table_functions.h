#pragma once

#include "table_functions.h"

namespace kuzu {
namespace function {

class BuiltInTableFunctions {

public:
    BuiltInTableFunctions() { registerTableFunctions(); }

    TableFunctionDefinition* mathTableFunction(const std::string& name);

private:
    void registerTableFunctions();

private:
    // TODO(Ziyi): Refactor vectorFunction/tableFunction to inherit from the same base class.
    std::unordered_map<std::string, std::unique_ptr<TableFunctionDefinition>> tableFunctions;
};

} // namespace function
} // namespace kuzu
