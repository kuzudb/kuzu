#pragma once

#include "column_predicate.h"
#include "common/enums/expression_type.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace storage {

class ColumnConstantPredicate : public ColumnPredicate {
public:
    ColumnConstantPredicate(common::ExpressionType expressionType, common::Value value)
        : expressionType{expressionType}, value{std::move(value)} {}

    common::ZoneMapCheckResult checkZoneMap(const CompressionMetadata& metadata) const override;

    std::unique_ptr<ColumnPredicate> copy() const override {
        return std::make_unique<ColumnConstantPredicate>(expressionType, value);
    }

private:
    common::ExpressionType expressionType;
    common::Value value;
};

} // namespace storage
} // namespace kuzu
