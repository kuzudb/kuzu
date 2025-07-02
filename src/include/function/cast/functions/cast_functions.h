#pragma once

#include "common/exception/overflow.h"
#include "common/string_format.h"
#include "common/type_utils.h"
#include "common/vector/value_vector.h"
#include "function/built_in_function_utils.h"
#include "function/cast/cast_function_bind_data.h"
#include "function/cast/functions/numeric_cast.h"
#include "function/cast/vector_cast_functions.h"

namespace kuzu {
namespace function {

struct CastToString {
    template<typename T>
    static inline void operation(T& input, common::ku_string_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto str = common::TypeUtils::toString(input, (void*)&inputVector);
        common::StringVector::addString(&resultVector, result, str);
    }
};

struct CastNodeToString {
    static inline void operation(common::struct_entry_t& input, common::ku_string_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto str = common::TypeUtils::nodeToString(input, &inputVector);
        common::StringVector::addString(&resultVector, result, str);
    }
};

struct CastRelToString {
    static inline void operation(common::struct_entry_t& input, common::ku_string_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto str = common::TypeUtils::relToString(input, &inputVector);
        common::StringVector::addString(&resultVector, result, str);
    }
};

struct CastToUnion {
    template<typename T>
    static inline void operation(T&, common::union_entry_t&, common::ValueVector& inputVector,
        common::ValueVector& resultVector) {
        auto* selVector = inputVector.getSelVectorPtr();
        const auto& sourceType = inputVector.dataType;
        const auto& targetType = resultVector.dataType;
        uint32_t minCastCost = UNDEFINED_CAST_COST;
        union_field_idx_t minCostField = 0;
        for (uint64_t i = 0; i < UnionType::getNumFields(targetType); ++i) {
            const auto& fieldType = UnionType::getFieldType(targetType, i);
            if (CastFunction::hasImplicitCast(sourceType, fieldType)) {
                uint32_t castCost = BuiltInFunctionsUtils::getCastCost(
                    sourceType.getLogicalTypeID(), fieldType.getLogicalTypeID());
                if (castCost < minCastCost) {
                    minCastCost = castCost;
                    minCostField = i;
                }
            }
        }
        auto* tagVector = UnionVector::getTagVector(&resultVector);
        auto* valVector = UnionVector::getValVector(&resultVector, minCostField);
        const auto& innerType = UnionType::getFieldType(targetType, minCostField);
        for (auto& pos : selVector->getSelectedPositions()) {
            tagVector->setValue<union_field_idx_t>(pos, minCostField);
        }
        if (sourceType != innerType) {
            auto innerCast =
                CastFunction::bindCastFunction<CastChildFunctionExecutor>("CAST", sourceType,
                    innerType);
            std::vector<std::shared_ptr<ValueVector>> innerParams{
                std::shared_ptr<ValueVector>(&inputVector, [](ValueVector*) {})};
            CastFunctionBindData innerBindData(innerType.copy());
            innerBindData.numOfEntries = (*selVector)[selVector->getSelSize() - 1] + 1;
            innerCast->execFunc(innerParams, SelectionVector::fromValueVectors(innerParams),
                *valVector, valVector->getSelVectorPtr(), &innerBindData);
        } else {
            for (auto& pos : inputVector.getSelVectorPtr()->getSelectedPositions()) {
                valVector->copyFromVectorData(pos, &inputVector, pos);
            }
        }
    }
};

struct CastDateToTimestamp {
    template<typename T>
    static inline void operation(common::date_t& input, T& result) {
        // base case: timestamp
        result = common::Timestamp::fromDateTime(input, common::dtime_t{});
    }
};

template<>
inline void CastDateToTimestamp::operation(common::date_t& input, common::timestamp_ns_t& result) {
    operation<common::timestamp_t>(input, result);
    result = common::timestamp_ns_t{common::Timestamp::getEpochNanoSeconds(result)};
}

template<>
inline void CastDateToTimestamp::operation(common::date_t& input, common::timestamp_ms_t& result) {
    operation<common::timestamp_t>(input, result);
    result.value /= common::Interval::MICROS_PER_MSEC;
}

template<>
inline void CastDateToTimestamp::operation(common::date_t& input, common::timestamp_sec_t& result) {
    operation<common::timestamp_t>(input, result);
    result.value /= common::Interval::MICROS_PER_SEC;
}

struct CastToDate {
    template<typename T>
    static inline void operation(T& input, common::date_t& result);
};

template<>
inline void CastToDate::operation(common::timestamp_t& input, common::date_t& result) {
    result = common::Timestamp::getDate(input);
}

template<>
inline void CastToDate::operation(common::timestamp_ns_t& input, common::date_t& result) {
    auto tmp = common::Timestamp::fromEpochNanoSeconds(input.value);
    operation<common::timestamp_t>(tmp, result);
}

template<>
inline void CastToDate::operation(common::timestamp_ms_t& input, common::date_t& result) {
    auto tmp = common::Timestamp::fromEpochMilliSeconds(input.value);
    operation<common::timestamp_t>(tmp, result);
}

template<>
inline void CastToDate::operation(common::timestamp_sec_t& input, common::date_t& result) {
    auto tmp = common::Timestamp::fromEpochSeconds(input.value);
    operation<common::timestamp_t>(tmp, result);
}

struct CastToDouble {
    template<typename T>
    static inline void operation(T& input, double& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within DOUBLE range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToDouble::operation(common::int128_t& input, double& result) {
    if (!common::Int128_t::tryCast(input, result)) { // LCOV_EXCL_START
        throw common::OverflowException{common::stringFormat("Value {} is not within DOUBLE range",
            common::TypeUtils::toString(input))};
    } // LCOV_EXCL_STOP
}

struct CastToFloat {
    template<typename T>
    static inline void operation(T& input, float& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within FLOAT range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToFloat::operation(common::int128_t& input, float& result) {
    if (!common::Int128_t::tryCast(input, result)) { // LCOV_EXCL_START
        throw common::OverflowException{common::stringFormat("Value {} is not within FLOAT range",
            common::TypeUtils::toString(input))};
    }; // LCOV_EXCL_STOP
}

struct CastToInt128 {
    template<typename T>
    static inline void operation(T& input, common::int128_t& result) {
        common::Int128_t::tryCastTo(input, result);
    }
};

struct CastToInt64 {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT64 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt64::operation(common::int128_t& input, int64_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within INT64 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToSerial {
    template<typename T>
    static inline void operation(T& input, int64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT64 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToSerial::operation(common::int128_t& input, int64_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within INT64 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToInt32 {
    template<typename T>
    static inline void operation(T& input, int32_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT32 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt32::operation(common::int128_t& input, int32_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within INT32 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToInt16 {
    template<typename T>
    static inline void operation(T& input, int16_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT16 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt16::operation(common::int128_t& input, int16_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within INT16 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToInt8 {
    template<typename T>
    static inline void operation(T& input, int8_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within INT8 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToInt8::operation(common::int128_t& input, int8_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within INT8 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToUInt64 {
    template<typename T>
    static inline void operation(T& input, uint64_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT64 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt64::operation(common::int128_t& input, uint64_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within UINT64 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToUInt32 {
    template<typename T>
    static inline void operation(T& input, uint32_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT32 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt32::operation(common::int128_t& input, uint32_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within UINT32 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToUInt16 {
    template<typename T>
    static inline void operation(T& input, uint16_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT16 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt16::operation(common::int128_t& input, uint16_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within UINT16 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastToUInt8 {
    template<typename T>
    static inline void operation(T& input, uint8_t& result) {
        if (!tryCastWithOverflowCheck(input, result)) {
            throw common::OverflowException{common::stringFormat(
                "Value {} is not within UINT8 range", common::TypeUtils::toString(input))};
        }
    }
};

template<>
inline void CastToUInt8::operation(common::int128_t& input, uint8_t& result) {
    if (!common::Int128_t::tryCast(input, result)) {
        throw common::OverflowException{common::stringFormat("Value {} is not within UINT8 range",
            common::TypeUtils::toString(input))};
    };
}

struct CastBetweenTimestamp {
    template<typename SRC_TYPE, typename DST_TYPE>
    static void operation(const SRC_TYPE& input, DST_TYPE& result) {
        // base case: same type
        result.value = input.value;
    }
};

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_t& input,
    common::timestamp_ns_t& output) {
    output.value = common::Timestamp::getEpochNanoSeconds(input);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_t& input,
    common::timestamp_ms_t& output) {
    output.value = common::Timestamp::getEpochMilliSeconds(input);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_t& input,
    common::timestamp_sec_t& output) {
    output.value = common::Timestamp::getEpochSeconds(input);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_ms_t& input,
    common::timestamp_t& output) {
    output = common::Timestamp::fromEpochMilliSeconds(input.value);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_ms_t& input,
    common::timestamp_ns_t& output) {
    operation<common::timestamp_ms_t, common::timestamp_t>(input, output);
    operation<common::timestamp_t, common::timestamp_ns_t>(output, output);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_ms_t& input,
    common::timestamp_sec_t& output) {
    operation<common::timestamp_ms_t, common::timestamp_t>(input, output);
    operation<common::timestamp_t, common::timestamp_sec_t>(output, output);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_ns_t& input,
    common::timestamp_t& output) {
    output = common::Timestamp::fromEpochNanoSeconds(input.value);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_ns_t& input,
    common::timestamp_ms_t& output) {
    operation<common::timestamp_ns_t, common::timestamp_t>(input, output);
    operation<common::timestamp_t, common::timestamp_ms_t>(output, output);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_ns_t& input,
    common::timestamp_sec_t& output) {
    operation<common::timestamp_ns_t, common::timestamp_t>(input, output);
    operation<common::timestamp_t, common::timestamp_sec_t>(output, output);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_sec_t& input,
    common::timestamp_t& output) {
    output = common::Timestamp::fromEpochSeconds(input.value);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_sec_t& input,
    common::timestamp_ns_t& output) {
    operation<common::timestamp_sec_t, common::timestamp_t>(input, output);
    operation<common::timestamp_t, common::timestamp_ns_t>(output, output);
}

template<>
inline void CastBetweenTimestamp::operation(const common::timestamp_sec_t& input,
    common::timestamp_ms_t& output) {
    operation<common::timestamp_sec_t, common::timestamp_t>(input, output);
    operation<common::timestamp_t, common::timestamp_ms_t>(output, output);
}

} // namespace function
} // namespace kuzu
