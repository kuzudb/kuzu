#pragma once

#include "binder/copy/bound_file_scan_info.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundLoadFrom : public BoundReadingClause {
public:
    explicit BoundLoadFrom(BoundFileScanInfo info)
        : BoundReadingClause{common::ClauseType::LOAD_FROM}, info{std::move(info)} {}

    inline const BoundFileScanInfo* getInfo() const { return &info; }

private:
    BoundFileScanInfo info;
};

} // namespace binder
} // namespace kuzu
