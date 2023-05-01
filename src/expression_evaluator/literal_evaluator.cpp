#include "expression_evaluator/literal_evaluator.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/vector/value_vector_utils.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace evaluator {

bool LiteralExpressionEvaluator::select(SelectionVector& selVector) {
    assert(resultVector->dataType.typeID == BOOL);
    auto pos = resultVector->state->selVector->selectedPositions[0];
    assert(pos == 0u);
    return resultVector->getValue<bool>(pos) && (!resultVector->isNull(pos));
}

void LiteralExpressionEvaluator::resolveResultVector(
    const processor::ResultSet& resultSet, MemoryManager* memoryManager) {
    resultVector = std::make_shared<ValueVector>(value->getDataType(), memoryManager);
    if (value->isNull()) {
        resultVector->setNull(0 /* pos */, true);
    } else {
        copyValueToVector(resultVector->getData(), resultVector.get(), value.get());
    }
    resultVector->state = DataChunkState::getSingleValueDataChunkState();
}

void LiteralExpressionEvaluator::copyValueToVector(
    uint8_t* dstValue, common::ValueVector* dstVector, const common::Value* srcValue) {
    auto numBytesPerValue = dstVector->getNumBytesPerValue();
    switch (srcValue->getDataType().typeID) {
    case common::INT64: {
        memcpy(dstValue, &srcValue->val.int64Val, numBytesPerValue);
    } break;
    case common::INT32: {
        memcpy(dstValue, &srcValue->val.int32Val, numBytesPerValue);
    } break;
    case common::INT16: {
        memcpy(dstValue, &srcValue->val.int16Val, numBytesPerValue);
    } break;
    case common::DOUBLE: {
        memcpy(dstValue, &srcValue->val.doubleVal, numBytesPerValue);
    } break;
    case common::FLOAT: {
        memcpy(dstValue, &srcValue->val.floatVal, numBytesPerValue);
    } break;
    case common::BOOL: {
        memcpy(dstValue, &srcValue->val.booleanVal, numBytesPerValue);
    } break;
    case common::DATE: {
        memcpy(dstValue, &srcValue->val.dateVal, numBytesPerValue);
    } break;
    case common::TIMESTAMP: {
        memcpy(dstValue, &srcValue->val.timestampVal, numBytesPerValue);
    } break;
    case common::INTERVAL: {
        memcpy(dstValue, &srcValue->val.intervalVal, numBytesPerValue);
    } break;
    case common::STRING: {
        common::InMemOverflowBufferUtils::copyString(srcValue->strVal.data(),
            srcValue->strVal.length(), *(common::ku_string_t*)dstValue,
            *common::StringVector::getInMemOverflowBuffer(dstVector));
    } break;
    case common::VAR_LIST: {
        auto listListEntry = reinterpret_cast<common::list_entry_t*>(dstValue);
        auto numValues = srcValue->nestedTypeVal.size();
        *listListEntry = common::ListVector::addList(dstVector, numValues);
        auto dstDataVector = common::ListVector::getDataVector(dstVector);
        auto dstElements = common::ListVector::getListValues(dstVector, *listListEntry);
        for (auto i = 0u; i < numValues; ++i) {
            copyValueToVector(dstElements + i * dstDataVector->getNumBytesPerValue(), dstDataVector,
                srcValue->nestedTypeVal[i].get());
        }
    } break;
    default:
        throw common::NotImplementedException("Unimplemented setLiteral() for type " +
                                              common::Types::dataTypeToString(dstVector->dataType));
    }
}

} // namespace evaluator
} // namespace kuzu
