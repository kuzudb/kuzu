#include "binder/bound_scan_source.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

expression_vector BoundTableScanSource::getWarningColumns() const {
    expression_vector warningDataExprs;
    for (common::column_id_t i = info.bindData->numWarningDataColumns; i >= 1; --i) {
        KU_ASSERT(i < info.columns.size());
        warningDataExprs.push_back(info.columns[info.columns.size() - i]);
    }
    return warningDataExprs;
}

bool BoundTableScanSource::getIgnoreErrorsOption() const {
    return info.bindData->getIgnoreErrorsOption();
}

} // namespace binder
} // namespace kuzu
