#include "function/cast/functions/cast_rdf_variant.h"

#include "common/exception/runtime.h"
#include "common/types/blob.h"
#include "function/cast/functions/cast_functions.h"
#include "function/cast/functions/numeric_cast.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename T>
static void castToNumeric(ValueVector& inputVector, uint64_t inputPos, T& result,
    ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto type = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    auto& blob = blobVector->getValue<blob_t>(inputPos);
    bool succeed = false;
    switch (type) {
    case LogicalTypeID::INT64: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<int64_t>(blob), result);
    } break;
    case LogicalTypeID::INT32: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<int32_t>(blob), result);
    } break;
    case LogicalTypeID::INT16: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<int16_t>(blob), result);
    } break;
    case LogicalTypeID::INT8: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<int8_t>(blob), result);
    } break;
    case LogicalTypeID::UINT64: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<uint64_t>(blob), result);
    } break;
    case LogicalTypeID::UINT32: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<uint32_t>(blob), result);
    } break;
    case LogicalTypeID::UINT16: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<uint16_t>(blob), result);
    } break;
    case LogicalTypeID::UINT8: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<uint8_t>(blob), result);
    } break;
    case LogicalTypeID::DOUBLE: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<double>(blob), result);
    } break;
    case LogicalTypeID::FLOAT: {
        succeed = tryCastWithOverflowCheck(Blob::getValue<float>(blob), result);
    } break;
    default:
        succeed = false;
    }
    resultVector.setNull(resultPos, !succeed);
}

// TODO(Xiyang): add cast to int128
// RDF_VARIANT -> NUMERIC
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    int64_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    int32_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    int16_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    int8_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    uint64_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    uint32_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    uint16_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    uint8_t& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    double& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    float& result, ValueVector& resultVector, uint64_t resultPos) {
    castToNumeric(inputVector, inputPos, result, resultVector, resultPos);
}

// RDF_VARIANT -> BLOB
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    blob_t&, ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto typeID = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    if (typeID == common::LogicalTypeID::BLOB) {
        auto blob = Blob::getValue<blob_t>(blobVector->getValue<blob_t>(inputPos));
        BlobVector::addBlob(&resultVector, resultPos, blob.value.getData(), blob.value.len);
        resultVector.setNull(resultPos, false);
    } else {
        resultVector.setNull(resultPos, true);
    }
}
// RDF_VARIANT -> BOOL
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    bool&, ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto typeID = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    if (typeID == common::LogicalTypeID::BOOL) {
        auto& blob = blobVector->getValue<blob_t>(inputPos);
        resultVector.setValue(resultPos, Blob::getValue<bool>(blob));
        resultVector.setNull(resultPos, false);
    } else {
        resultVector.setNull(resultPos, true);
    }
}
// RDF_VARIANT -> DATE
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    date_t&, ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto typeID = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    if (typeID == common::LogicalTypeID::DATE) {
        auto& blob = blobVector->getValue<blob_t>(inputPos);
        resultVector.setValue(resultPos, Blob::getValue<date_t>(blob));
        resultVector.setNull(resultPos, false);
    } else {
        resultVector.setNull(resultPos, true);
    }
}
// RDF_VARIANT -> TIMESTAMP
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    timestamp_t&, ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto typeID = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    if (typeID == common::LogicalTypeID::TIMESTAMP) {
        auto& blob = blobVector->getValue<blob_t>(inputPos);
        resultVector.setValue(resultPos, Blob::getValue<timestamp_t>(blob));
        resultVector.setNull(resultPos, false);
    } else {
        resultVector.setNull(resultPos, true);
    }
}
// RDF_VARIANT -> INTERVAL
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    interval_t&, ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto typeID = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    if (typeID == common::LogicalTypeID::INTERVAL) {
        auto& blob = blobVector->getValue<blob_t>(inputPos);
        resultVector.setValue(resultPos, Blob::getValue<interval_t>(blob));
        resultVector.setNull(resultPos, false);
    } else {
        resultVector.setNull(resultPos, true);
    }
}

// RDF_VARIANT -> STRING
template<>
void CastFromRdfVariant::operation(struct_entry_t&, ValueVector& inputVector, uint64_t inputPos,
    ku_string_t& result, ValueVector& resultVector, uint64_t resultPos) {
    auto typeVector = StructVector::getFieldVector(&inputVector, 0).get();
    auto blobVector = StructVector::getFieldVector(&inputVector, 1).get();
    auto type = static_cast<LogicalTypeID>(typeVector->getValue<uint8_t>(inputPos));
    auto& blob = blobVector->getValue<blob_t>(inputPos);
    switch (type) {
    case LogicalTypeID::INT64: {
        auto val = Blob::getValue<int64_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::INT32: {
        auto val = Blob::getValue<int32_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::INT16: {
        auto val = Blob::getValue<int16_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::INT8: {
        auto val = Blob::getValue<int8_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::UINT64: {
        auto val = Blob::getValue<uint64_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::UINT32: {
        auto val = Blob::getValue<uint32_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::UINT16: {
        auto val = Blob::getValue<uint16_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::UINT8: {
        auto val = Blob::getValue<uint8_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::DOUBLE: {
        auto val = Blob::getValue<double>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::FLOAT: {
        auto val = Blob::getValue<float>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::BOOL: {
        auto val = Blob::getValue<bool>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::DATE: {
        auto val = Blob::getValue<date_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        auto val = Blob::getValue<timestamp_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::INTERVAL: {
        auto val = Blob::getValue<interval_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    case LogicalTypeID::STRING: {
        result = blobVector->getValue<blob_t>(inputPos).value;
    } break;
    case LogicalTypeID::BLOB: {
        auto val = Blob::getValue<blob_t>(blob);
        CastToString::operation(val, result, *blobVector, resultVector);
    } break;
    default:
        throw RuntimeException(
            stringFormat("CastFromRdfVariant::operation on type {} is not implemented.",
                LogicalTypeUtils::toString(type)));
    }
    resultVector.setNull(resultPos, false);
}

} // namespace function
} // namespace kuzu
