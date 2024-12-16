#pragma once

#include "column_predicate.h"
#include "common/enums/expression_type.h"

namespace kuzu {
namespace storage {

class ColumnIsNullPredicate : public ColumnPredicate {
public:
    explicit ColumnIsNullPredicate(std::string columnName)
        : ColumnPredicate{std::move(columnName), common::ExpressionType::IS_NULL} {}

    common::ZoneMapCheckResult checkZoneMap(const ColumnChunkStats& stats) const override;

    std::unique_ptr<ColumnPredicate> copy() const override {
        return std::make_unique<ColumnIsNullPredicate>(columnName);
    }
};

class ColumnIsNotNullPredicate : public ColumnPredicate {
public:
    explicit ColumnIsNotNullPredicate(std::string columnName)
        : ColumnPredicate{std::move(columnName), common::ExpressionType::IS_NOT_NULL} {}

    common::ZoneMapCheckResult checkZoneMap(const ColumnChunkStats& stats) const override;

    std::unique_ptr<ColumnPredicate> copy() const override {
        return std::make_unique<ColumnIsNotNullPredicate>(columnName);
    }
};

} // namespace storage
} // namespace kuzu
