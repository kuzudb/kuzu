#pragma once

#include "src/common/include/type_utils.h"
#include "src/common/types/include/date_t.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Century {
    static inline void operation(timestamp_t& timestamp, bool isNull, int64_t& result) {
        assert(!isNull);
        result = Timestamp::getTimestampPart(DatePartSpecifier::CENTURY, timestamp);
    }
};

struct EpochMs {
    static inline void operation(int64_t& ms, bool isNull, timestamp_t& result) {
        assert(!isNull);
        result = Timestamp::FromEpochMs(ms);
    }
};

struct ToTimestamp {
    static inline void operation(int64_t& sec, bool isNull, timestamp_t& result) {
        assert(!isNull);
        result = Timestamp::FromEpochSec(sec);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
