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
    bool ignoreErrors = common::CopyConstants::DEFAULT_IGNORE_ERRORS;
    if (type == common::ScanSourceType::FILE) {
        auto bindData = info.bindData->constPtrCast<function::ScanBindData>();
        ignoreErrors = bindData->config.getOption(common::CopyConstants::IGNORE_ERRORS_OPTION_NAME,
            ignoreErrors);
    }
    return ignoreErrors;
}

} // namespace binder
} // namespace kuzu
