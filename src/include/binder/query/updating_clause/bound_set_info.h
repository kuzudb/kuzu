#pragma once

#include "binder/expression/expression.h"
#include "common/enums/table_type.h"

namespace kuzu {
namespace binder {

struct BoundSetPkInfo {
    expression_vector columnDataExprs;

    explicit BoundSetPkInfo(expression_vector columnDataExprs)
        : columnDataExprs{std::move(columnDataExprs)} {}

    std::unique_ptr<BoundSetPkInfo> copy() const {
        return std::make_unique<BoundSetPkInfo>(columnDataExprs);
    }
};

struct BoundSetPropertyInfo {
    common::TableType tableType;
    std::shared_ptr<Expression> pattern;
    std::shared_ptr<Expression> column;
    std::shared_ptr<Expression> columnData;
    std::unique_ptr<BoundSetPkInfo> pkInfo = nullptr;

    BoundSetPropertyInfo(common::TableType tableType, std::shared_ptr<Expression> pattern,
        std::shared_ptr<Expression> column, std::shared_ptr<Expression> columnData)
        : tableType{tableType}, pattern{std::move(pattern)}, column{std::move(column)},
          columnData{std::move(columnData)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundSetPropertyInfo);

private:
    BoundSetPropertyInfo(const BoundSetPropertyInfo& other)
        : tableType{other.tableType}, pattern{other.pattern}, column{other.column},
          columnData{other.columnData} {
        if (other.pkInfo != nullptr) {
            pkInfo =  other.pkInfo->copy();
        }
    }
};

} // namespace binder
} // namespace kuzu
