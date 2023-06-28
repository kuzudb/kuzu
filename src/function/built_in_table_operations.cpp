#include "function/built_in_table_operations.h"

#include "common/expression_type.h"
#include "common/string_utils.h"

namespace kuzu {
namespace function {

void BuiltInTableOperations::registerTableOperations() {
    tableOperations.insert({common::TABLE_INFO_FUNC_NAME, TableInfoOperation::getDefinitions()});
}

TableOperationDefinition* BuiltInTableOperations::mathTableOperation(const std::string& name) {
    auto upperName = name;
    common::StringUtils::toUpper(upperName);
    if (!tableOperations.contains(upperName)) {
        throw common::BinderException{
            "Cannot match a built-in function for given function " + name + "."};
    }
    return tableOperations.at(upperName).get();
}

} // namespace function
} // namespace kuzu
