#include "function/built_in_table_functions.h"

#include "common/expression_type.h"
#include "common/string_utils.h"

namespace kuzu {
namespace function {

void BuiltInTableFunctions::registerTableFunctions() {
    tableFunctions.insert({common::TABLE_INFO_FUNC_NAME, TableInfoFunction::getDefinitions()});
    tableFunctions.insert({common::DB_VERSION_FUNC_NAME, DBVersionFunction::getDefinitions()});
    tableFunctions.insert(
        {common::CURRENT_SETTING_FUNC_NAME, CurrentSettingFunction::getDefinitions()});
}

TableFunctionDefinition* BuiltInTableFunctions::mathTableFunction(const std::string& name) {
    auto upperName = name;
    common::StringUtils::toUpper(upperName);
    if (!tableFunctions.contains(upperName)) {
        throw common::BinderException{
            "Cannot match a built-in function for given function " + name + "."};
    }
    return tableFunctions.at(upperName).get();
}

} // namespace function
} // namespace kuzu
