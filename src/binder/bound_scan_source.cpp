#include "binder/bound_scan_source.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

expression_vector BoundTableScanSource::getWarningColumns() const {
    expression_vector warningDataExprs;
    auto& columns = info.bindData->columns;
    for (common::column_id_t i = info.bindData->numWarningDataColumns; i >= 1; --i) {
        KU_ASSERT(i < columns.size());
        warningDataExprs.push_back(columns[columns.size() - i]);
    }
    return warningDataExprs;
}

bool BoundTableScanSource::getIgnoreErrorsOption() const {
    return info.bindData->getIgnoreErrorsOption();
}

bool BoundQueryScanSource::getIgnoreErrorsOption() const {
    return info.options.contains(CopyConstants::IGNORE_ERRORS_OPTION_NAME) ?
               info.options.at(CopyConstants::IGNORE_ERRORS_OPTION_NAME).getValue<bool>() :
               CopyConstants::DEFAULT_IGNORE_ERRORS;
}

} // namespace binder
} // namespace kuzu
