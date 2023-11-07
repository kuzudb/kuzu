#include "function/cast/functions/cast_rdf_variant.h"

#include "common/types/blob.h"
#include "function/cast/functions/cast_functions.h"
#include "function/cast/functions/numeric_cast.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename T>
static void castToNumeric(common::ValueVector& inputVector, uint64_t inputPos, T& result,
    common::ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto type = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    switch (type) {
    case LogicalTypeID::INT64: {
        auto val = Blob::getValue<int64_t>(blobVector->getValue<blob_t>(inputPos));
        if (!tryCastWithOverflowCheck(val, result)) {
            resultVector.setNull(resultPos, true);
        }
    } break;
    case LogicalTypeID::DOUBLE: {
        auto val = Blob::getValue<double_t>(blobVector->getValue<blob_t>(inputPos));
        if (!tryCastWithOverflowCheck(val, result)) {
            resultVector.setNull(resultPos, true);
        }
    } break;
    default:
        resultVector.setNull(resultPos, true);
    }
}

// TODO(Xiyang): add cast to int128
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    int64_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    int32_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    int16_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    int8_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    uint64_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    uint32_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    uint16_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    uint8_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    double_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    float_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}

template<typename T>
static void castFromString(common::ValueVector& /*inputVector*/, uint64_t /*inputPos*/,
    T& /*result*/, common::ValueVector& resultVector, uint64_t resultPos) {
    resultVector.setNull(resultPos, true);
}

template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    blob_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castFromString(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    bool& result, common::ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto type = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    switch (type) {
    case LogicalTypeID::BOOL: {
        result = Blob::getValue<bool>(blobVector->getValue<blob_t>(inputPos));
    } break;
    default:
        resultVector.setNull(resultPos, true);
    }
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    date_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto type = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    switch (type) {
    case LogicalTypeID::DATE: {
        result = Blob::getValue<date_t>(blobVector->getValue<blob_t>(inputPos));
    } break;
    default:
        resultVector.setNull(resultPos, true);
    }
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    timestamp_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castFromString(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    interval_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    castFromString(inputVector, inputPos, result, resultVector, resultPos);
}

template<>
void CastFromRdfVariant::operation(common::ValueVector& inputVector, uint64_t inputPos,
    ku_string_t& result, common::ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto type = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    switch (type) {
    case LogicalTypeID::INT64: {
        int64_t val = Blob::getValue<int64_t>(blobVector->getValue<blob_t>(inputPos));
        CastToString::operation(val, result, inputVector, resultVector);
    } break;
    case LogicalTypeID::DOUBLE: {
        auto val = Blob::getValue<double_t>(blobVector->getValue<blob_t>(inputPos));
        CastToString::operation(val, result, inputVector, resultVector);
    } break;
    case LogicalTypeID::BOOL: {
        auto val = Blob::getValue<bool>(blobVector->getValue<blob_t>(inputPos));
        CastToString::operation(val, result, inputVector, resultVector);
    } break;
    case LogicalTypeID::DATE: {
        auto val = Blob::getValue<date_t>(blobVector->getValue<blob_t>(inputPos));
        CastToString::operation(val, result, inputVector, resultVector);
    } break;
    case LogicalTypeID::STRING: {
        result = blobVector->getValue<blob_t>(inputPos).value;
    } break;
    default:
        resultVector.setNull(resultPos, true);
    }
}

} // namespace function
} // namespace kuzu
