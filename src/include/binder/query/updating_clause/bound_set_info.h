#pragma once

#include "binder/expression/expression.h"
#include "common/enums/table_type.h"

namespace kuzu {
namespace binder {

struct BoundSetPropertyInfo {
    common::TableType tableType;
    std::shared_ptr<Expression> pattern;
    expression_pair setItem;
    std::shared_ptr<Expression> pkExpr = nullptr;

    BoundSetPropertyInfo(common::TableType tableType, std::shared_ptr<Expression> pattern,
        expression_pair setItem)
        : tableType{tableType}, pattern{std::move(pattern)}, setItem{std::move(setItem)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundSetPropertyInfo);

private:
    BoundSetPropertyInfo(const BoundSetPropertyInfo& other)
        : tableType{other.tableType}, pattern{other.pattern}, setItem{other.setItem},
          pkExpr{other.pkExpr} {}
};

} // namespace binder
} // namespace kuzu
