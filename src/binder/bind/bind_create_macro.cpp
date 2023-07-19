#include "binder/binder.h"
#include "binder/macro/bound_create_macro.h"
#include "common/string_utils.h"
#include "parser/macro/create_macro.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCreateMacro(const parser::Statement& statement) {
    auto& createMacro = reinterpret_cast<const parser::CreateMacro&>(statement);
    auto macroName = createMacro.getMacroName();
    if (catalog.getReadOnlyVersion()->containMacro(macroName)) {
        throw common::BinderException{
            common::StringUtils::string_format("Macro {} already exists.", macroName)};
    }
    parser::default_macro_args defaultArgs;
    for (auto& defaultArg : createMacro.getDefaultArgs()) {
        defaultArgs.emplace_back(defaultArg.first, defaultArg.second->copy());
    }
    auto scalarMacro =
        std::make_unique<function::ScalarMacroFunction>(createMacro.getMacroExpression()->copy(),
            createMacro.getPositionalArgs(), std::move(defaultArgs));
    return std::make_unique<BoundCreateMacro>(macroName, std::move(scalarMacro));
}

} // namespace binder
} // namespace kuzu
