#pragma once

#include "table_operations.h"

namespace kuzu {
namespace function {

class BuiltInTableOperations {

public:
    BuiltInTableOperations() { registerTableOperations(); }

    TableOperationDefinition* mathTableOperation(const std::string& name);

private:
    void registerTableOperations();

private:
    // TODO(Ziyi): Refactor vectorOperation/tableOperation to inherit from the same base class.
    std::unordered_map<std::string, std::unique_ptr<TableOperationDefinition>> tableOperations;
};

} // namespace function
} // namespace kuzu
