#pragma once

#include "common/exception/conversion.h"
#include "common/types/date_t.h"
#include "common/types/interval_t.h"
#include "common/types/timestamp_t.h"
#include "function/cast/functions/numeric_cast.h"

namespace kuzu {
namespace function {

struct Century {
    static inline void operation(common::timestamp_t& timestamp, int64_t& result) {
        result = common::Timestamp::getTimestampPart(common::DatePartSpecifier::CENTURY, timestamp);
    }
};

struct EpochMs {
    static inline void operation(int64_t& ms, common::timestamp_t& result) {
        result = common::Timestamp::fromEpochMilliSeconds(ms);
    }
};

struct ToTimestamp {
    static inline void operation(double_t& sec, common::timestamp_t& result) {
        int64_t ms;
        if (!tryCastWithOverflowCheck(sec * common::Interval::MICROS_PER_SEC, ms)) {
            throw common::ConversionException("Could not convert epoch seconds to TIMESTAMP");
        }
        result = common::timestamp_t(ms);
    }
};

} // namespace function
} // namespace kuzu
