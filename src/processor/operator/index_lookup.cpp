#include "processor/operator/index_lookup.h"

#include "common/exception/message.h"
#include "common/exception/not_implemented.h"
#include "common/exception/runtime.h"
#include "storage/index/hash_index.h"
#include "storage/store/table_copy_utils.h"
#include <arrow/array/array_base.h>
#include <arrow/type.h>

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

bool IndexLookup::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& info : infos) {
        assert(info);
        indexLookup(*info);
    }
    return true;
}

std::unique_ptr<PhysicalOperator> IndexLookup::clone() {
    std::vector<std::unique_ptr<IndexLookupInfo>> copiedInfos;
    copiedInfos.reserve(infos.size());
    for (const auto& info : infos) {
        copiedInfos.push_back(info->copy());
    }
    return make_unique<IndexLookup>(
        std::move(copiedInfos), children[0]->clone(), getOperatorID(), paramsString);
}

void IndexLookup::indexLookup(const IndexLookupInfo& info) {
    auto keyVector = resultSet->getValueVector(info.keyVectorPos).get();
    arrow::ArrayVector offsetArraysVector;
    if (keyVector->dataType.getPhysicalType() == PhysicalTypeID::ARROW_COLUMN) {
        fillOffsetArraysFromArrowVector(info, keyVector, offsetArraysVector);
    } else {
        fillOffsetArraysFromVector(info, keyVector, offsetArraysVector);
    }
    auto offsetChunkedArray = std::make_shared<arrow::ChunkedArray>(offsetArraysVector);
    ArrowColumnVector::setArrowColumn(
        resultSet->getValueVector(info.resultVectorPos).get(), offsetChunkedArray);
}

void IndexLookup::lookupOnArray(
    const IndexLookupInfo& info, arrow::Array* array, common::offset_t* offsets) {
    std::string errorPKValueStr;
    auto length = array->length();
    switch (info.pkDataType->getLogicalTypeID()) {
    case LogicalTypeID::SERIAL: {
        for (auto i = 0u; i < length; i++) {
            offsets[i] = static_cast<offset_t>(dynamic_cast<arrow::Int64Array*>(array)->Value(i));
        }
    } break;
    case LogicalTypeID::INT64: {
        auto numKeysFound = 0u;
        for (auto i = 0u; i < length; i++) {
            auto val = dynamic_cast<arrow::Int64Array*>(array)->Value(i);
            numKeysFound +=
                info.pkIndex->lookup(&transaction::DUMMY_READ_TRANSACTION, val, offsets[i]);
        }
        if (numKeysFound != length) {
            for (auto i = 0u; i < length; i++) {
                auto val = dynamic_cast<arrow::Int64Array*>(array)->Value(i);
                if (!info.pkIndex->lookup(&transaction::DUMMY_READ_TRANSACTION, val, offsets[i])) {
                    errorPKValueStr = std::to_string(val);
                    break;
                }
            }
        }
    } break;
    case LogicalTypeID::STRING: {
        auto numKeysFound = 0u;
        for (auto i = 0u; i < length; i++) {
            auto val = std::string(dynamic_cast<arrow::StringArray*>(array)->GetView(i));
            numKeysFound +=
                info.pkIndex->lookup(&transaction::DUMMY_READ_TRANSACTION, val.c_str(), offsets[i]);
        }
        if (numKeysFound != length) {
            for (auto i = 0u; i < length; i++) {
                auto val = std::string(dynamic_cast<arrow::StringArray*>(array)->GetView(i));
                if (!info.pkIndex->lookup(
                        &transaction::DUMMY_READ_TRANSACTION, val.c_str(), offsets[i])) {
                    errorPKValueStr = val;
                    break;
                }
            }
        }
    } break;
        // LCOV_EXCL_START
    default: {
        throw NotImplementedException(ExceptionMessage::invalidPKType(
            LogicalTypeUtils::dataTypeToString(info.pkDataType->getLogicalTypeID())));
    }
        // LCOV_EXCL_STOP
    }
    if (!errorPKValueStr.empty()) {
        throw RuntimeException(ExceptionMessage::nonExistPKException(errorPKValueStr));
    }
}

void IndexLookup::fillOffsetArraysFromArrowVector(const IndexLookupInfo& info,
    common::ValueVector* keyVector, arrow::ArrayVector& offsetArraysVector) {
    // This should be changed to handle non-arrow Vectors once the new csv reader is in.
    auto keyChunkedArray = ArrowColumnVector::getArrowColumn(keyVector);
    offsetArraysVector.reserve(keyChunkedArray->num_chunks());
    for (auto& keyArray : keyChunkedArray->chunks()) {
        auto numKeys = keyArray->length();
        std::shared_ptr<arrow::Buffer> arrowBuffer;
        TableCopyUtils::throwCopyExceptionIfNotOK(
            arrow::AllocateBuffer((int64_t)(numKeys * sizeof(offset_t))).Value(&arrowBuffer));
        auto offsets = (offset_t*)arrowBuffer->data();
        if (keyArray->null_count() != 0) {
            throw RuntimeException(ExceptionMessage::nullPKException());
        }
        lookupOnArray(info, keyArray.get(), offsets);
        offsetArraysVector.push_back(TableCopyUtils::createArrowPrimitiveArray(
            std::make_shared<arrow::Int64Type>(), arrowBuffer, numKeys));
    }
}

void IndexLookup::fillOffsetArraysFromVector(const kuzu::processor::IndexLookupInfo& info,
    common::ValueVector* keyVector,
    std::vector<std::shared_ptr<arrow::Array>>& offsetArraysVector) {
    auto numKeys = keyVector->state->selVector->selectedSize;
    std::shared_ptr<arrow::Buffer> arrowBuffer;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        arrow::AllocateBuffer((int64_t)(numKeys * sizeof(offset_t))).Value(&arrowBuffer));
    auto offsets = (offset_t*)arrowBuffer->data();
    for (auto i = 0u; i < keyVector->state->selVector->selectedSize; i++) {
        info.pkIndex->lookup(&transaction::DUMMY_READ_TRANSACTION,
            keyVector->getValue<ku_string_t>(keyVector->state->selVector->selectedPositions[i])
                .getAsString()
                .c_str(),
            offsets[i]);
    }
    offsetArraysVector.push_back(TableCopyUtils::createArrowPrimitiveArray(
        std::make_shared<arrow::Int64Type>(), arrowBuffer, numKeys));
}

} // namespace processor
} // namespace kuzu
