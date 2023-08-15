#include "expression_evaluator/literal_evaluator.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

bool LiteralExpressionEvaluator::select(SelectionVector& selVector) {
    assert(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    assert(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& resultSet, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(*value->getDataType(), memoryManager);
    if (value->isNull()) {
        resultVector->setNull(0 /* pos */, true);
    } else {
        copyValueToVector(resultVector->getData(), resultVector.get(), value.get());
    }
    resultVector->setState(DataChunkState::getSingleValueDataChunkState());
}

void LiteralExpressionEvaluator::copyValueToVector(
    uint8_t* dstValue, ValueVector* dstVector, const Value* srcValue) {
    auto numBytesPerValue = dstVector->getNumBytesPerValue();
    switch (srcValue->getDataType()->getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        memcpy(dstValue, &srcValue->val.int64Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::INT32: {
        memcpy(dstValue, &srcValue->val.int32Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::INT16: {
        memcpy(dstValue, &srcValue->val.int16Val, numBytesPerValue);
    } break;
    case PhysicalTypeID::DOUBLE: {
        memcpy(dstValue, &srcValue->val.doubleVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::FLOAT: {
        memcpy(dstValue, &srcValue->val.floatVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::BOOL: {
        memcpy(dstValue, &srcValue->val.booleanVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::INTERVAL: {
        memcpy(dstValue, &srcValue->val.intervalVal, numBytesPerValue);
    } break;
    case PhysicalTypeID::STRING: {
        StringVector::addString(
            dstVector, *(ku_string_t*)dstValue, srcValue->strVal.data(), srcValue->strVal.length());
    } break;
    case PhysicalTypeID::VAR_LIST: {
        auto listListEntry = reinterpret_cast<list_entry_t*>(dstValue);
        auto numValues = NestedVal::getChildrenSize(srcValue);
        *listListEntry = ListVector::addList(dstVector, numValues);
        auto dstDataVector = ListVector::getDataVector(dstVector);
        auto dstElements = ListVector::getListValues(dstVector, *listListEntry);
        for (auto i = 0u; i < numValues; ++i) {
            copyValueToVector(dstElements + i * dstDataVector->getNumBytesPerValue(), dstDataVector,
                NestedVal::getChildVal(srcValue, i));
        }
    } break;
    default:
        throw NotImplementedException("Unimplemented setLiteral() for type " +
                                      LogicalTypeUtils::dataTypeToString(dstVector->dataType));
    }
}

} // namespace evaluator
} // namespace kuzu
