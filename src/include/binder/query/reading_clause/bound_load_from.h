#pragma once

#include "binder/copy/bound_table_scan_info.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundLoadFrom : public BoundReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::LOAD_FROM;

public:
    explicit BoundLoadFrom(BoundTableScanSourceInfo info)
        : BoundReadingClause{clauseType_}, info{std::move(info)} {}

    const BoundTableScanSourceInfo* getInfo() const { return &info; }

private:
    BoundTableScanSourceInfo info;
};

} // namespace binder
} // namespace kuzu
