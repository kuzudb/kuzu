#include "function/table/bind_input.h"

#include "binder/expression/literal_expression.h"

namespace kuzu {
namespace function {

void TableFuncBindInput::addLiteralParam(common::Value value) {
    params.push_back(std::make_shared<binder::LiteralExpression>(std::move(value), ""));
}

template<typename T>
T TableFuncBindInput::getLiteralVal(common::idx_t idx) const {
    KU_ASSERT(params[idx]->expressionType == common::ExpressionType::LITERAL);
    return params[idx]->constCast<binder::LiteralExpression>().getValue().getValue<T>();
}

template KUZU_API std::string TableFuncBindInput::getLiteralVal<std::string>(
    common::idx_t idx) const;
template KUZU_API uint64_t TableFuncBindInput::getLiteralVal<uint64_t>(common::idx_t idx) const;
template KUZU_API uint32_t TableFuncBindInput::getLiteralVal<uint32_t>(common::idx_t idx) const;

} // namespace function
} // namespace kuzu
