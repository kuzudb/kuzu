#include "py_scan_config.h"

#include "common/constants.h"
#include "common/exception/binder.h"
#include "function/cast/functions/numeric_limits.h"

namespace kuzu {

PyScanConfig::PyScanConfig(const common::case_insensitive_map_t<common::Value>& options,
    uint64_t numRows) {
    skipNum = 0;
    limitNum = function::NumericLimits<uint64_t>::maximum();
    ignoreErrors = common::CopyConstants::DEFAULT_IGNORE_ERRORS;
    for (const auto& i : options) {
        if (i.first == "SKIP") {
            if (i.second.getDataType().getLogicalTypeID() != common::LogicalTypeID::INT64 ||
                i.second.val.int64Val < 0) {
                throw common::BinderException("SKIP Option must be a positive integer literal.");
            }
            skipNum = std::min(numRows, static_cast<uint64_t>(i.second.val.int64Val));
        } else if (i.first == "LIMIT") {
            if (i.second.getDataType().getLogicalTypeID() != common::LogicalTypeID::INT64 ||
                i.second.val.int64Val < 0) {
                throw common::BinderException("LIMIT Option must be a positive integer literal.");
            }
            limitNum = i.second.val.int64Val;
        } else if (i.first == common::CopyConstants::IGNORE_ERRORS_OPTION_NAME) {
            if (i.second.getDataType().getLogicalTypeID() != common::LogicalTypeID::BOOL) {
                throw common::BinderException("IGNORE_ERRORS Option must be a boolean.");
            }
            ignoreErrors = i.second.val.booleanVal;
        } else {
            throw common::BinderException(
                common::stringFormat("{} Option not recognized by pyArrow scanner.", i.first));
        }
    }
}

} // namespace kuzu
