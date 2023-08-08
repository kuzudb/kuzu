#pragma once

#include "common/type_utils.h"
#include "common/types/date_t.h"

namespace kuzu {
namespace function {

struct Century {
    static inline void operation(common::timestamp_t& timestamp, int64_t& result) {
        result = common::Timestamp::getTimestampPart(common::DatePartSpecifier::CENTURY, timestamp);
    }
};

struct EpochMs {
    static inline void operation(int64_t& ms, common::timestamp_t& result) {
        result = common::Timestamp::fromEpochMs(ms);
    }
};

struct ToTimestamp {
    static inline void operation(int64_t& sec, common::timestamp_t& result) {
        result = common::Timestamp::fromEpochSec(sec);
    }
};

} // namespace function
} // namespace kuzu
