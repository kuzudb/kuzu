#pragma once

#include "binder/copy/bound_file_scan_info.h"
#include "bound_reading_clause.h"

namespace kuzu {
namespace binder {

class BoundLoadFrom : public BoundReadingClause {
public:
    BoundLoadFrom(std::unique_ptr<BoundFileScanInfo> info)
        : BoundReadingClause{common::ClauseType::LOAD_FROM}, info{std::move(info)} {}

    inline BoundFileScanInfo* getInfo() const { return info.get(); }

    inline std::unique_ptr<BoundReadingClause> copy() override {
        return std::make_unique<BoundLoadFrom>(info->copy());
    }

private:
    std::unique_ptr<BoundFileScanInfo> info;
};

} // namespace binder
} // namespace kuzu
