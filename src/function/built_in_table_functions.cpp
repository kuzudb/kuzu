#include "function/built_in_table_functions.h"

#include "common/exception/binder.h"
#include "common/expression_type.h"
#include "common/string_utils.h"
#include "function/table_functions/call_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void BuiltInTableFunctions::registerTableFunctions() {
    tableFunctions.insert({CURRENT_SETTING_FUNC_NAME, CurrentSettingFunction::getDefinitions()});
    tableFunctions.insert({DB_VERSION_FUNC_NAME, DBVersionFunction::getDefinitions()});
    tableFunctions.insert({SHOW_TABLES_FUNC_NAME, ShowTablesFunction::getDefinitions()});
    tableFunctions.insert({TABLE_INFO_FUNC_NAME, TableInfoFunction::getDefinitions()});
    tableFunctions.insert({SHOW_CONNECTION_FUNC_NAME, ShowConnectionFunction::getDefinitions()});
}

TableFunctionDefinition* BuiltInTableFunctions::mathTableFunction(const std::string& name) {
    auto upperName = name;
    StringUtils::toUpper(upperName);
    if (!tableFunctions.contains(upperName)) {
        throw BinderException{"Cannot match a built-in function for given function " + name + "."};
    }
    return tableFunctions.at(upperName).get();
}

} // namespace function
} // namespace kuzu
