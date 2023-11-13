#include "processor/operator/order_by/top_k.h"

#include "function/binary_function_executor.h"
#include "function/comparison/comparison_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

TopKSortState::TopKSortState() : numTuples{0}, memoryManager{nullptr} {
    orderByLocalState = std::make_unique<SortLocalState>();
    orderBySharedState = std::make_unique<SortSharedState>();
}

void TopKSortState::init(
    const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager) {
    this->memoryManager = memoryManager;
    orderBySharedState->init(orderByDataInfo);
    orderByLocalState->init(orderByDataInfo, *orderBySharedState, memoryManager);
    numTuples = 0;
}

void TopKSortState::append(const std::vector<common::ValueVector*>& keyVectors,
    const std::vector<common::ValueVector*>& payloadVectors) {
    numTuples += keyVectors[0]->state->selVector->selectedSize;
    orderByLocalState->append(keyVectors, payloadVectors);
}

void TopKSortState::finalize() {
    orderByLocalState->finalize(*orderBySharedState);
    auto merger = std::make_unique<KeyBlockMerger>(orderBySharedState->getPayloadTables(),
        orderBySharedState->getStrKeyColInfo(), orderBySharedState->getNumBytesPerTuple());
    auto dispatcher = std::make_unique<KeyBlockMergeTaskDispatcher>();
    dispatcher->init(memoryManager, orderBySharedState->getSortedKeyBlocks(),
        orderBySharedState->getPayloadTables(), orderBySharedState->getStrKeyColInfo(),
        orderBySharedState->getNumBytesPerTuple());
    while (!dispatcher->isDoneMerge()) {
        auto keyBlockMergeMorsel = dispatcher->getMorsel();
        merger->mergeKeyBlocks(*keyBlockMergeMorsel);
        dispatcher->doneMorsel(std::move(keyBlockMergeMorsel));
    }
}

void TopKBuffer::init(
    storage::MemoryManager* memoryManager, uint64_t skipNumber, uint64_t limitNumber) {
    this->memoryManager = memoryManager;
    sortState->init(*orderByDataInfo, memoryManager);
    this->skip = skipNumber;
    this->limit = limitNumber;
    initVectors();
    initCompareFuncs();
}

void TopKBuffer::append(const std::vector<common::ValueVector*>& keyVectors,
    const std::vector<common::ValueVector*>& payloadVectors) {
    auto originalSelState = keyVectors[0]->state->selVector;
    if (hasBoundaryValue && !compareBoundaryValue(keyVectors)) {
        keyVectors[0]->state->selVector = std::move(originalSelState);
        return;
    }
    sortState->append(keyVectors, payloadVectors);
    keyVectors[0]->state->selVector = std::move(originalSelState);
}

void TopKBuffer::reduce() {
    auto reduceThreshold = std::max(OrderByConstants::MIN_SIZE_TO_REDUCE,
        OrderByConstants::MIN_LIMIT_RATIO_TO_REDUCE * (limit + skip));
    if (sortState->getNumTuples() < reduceThreshold) {
        return;
    }
    sortState->finalize();
    auto newSortState = std::make_unique<TopKSortState>();
    newSortState->init(*orderByDataInfo, memoryManager);
    auto scanner = sortState->getScanner(0, skip + limit);
    while (true) {
        auto numTuplesScanned = scanner->scan(payloadVecsToScan);
        if (numTuplesScanned == 0) {
            setBoundaryValue();
            break;
        }
        newSortState->append(keyVecsToScan, payloadVecsToScan);
        std::swap(payloadVecsToScan, lastPayloadVecsToScan);
        std::swap(keyVecsToScan, lastKeyVecsToScan);
    }
    sortState = std::move(newSortState);
}

void TopKBuffer::merge(TopKBuffer* other) {
    other->finalize();
    if (other->sortState->getSharedState()->getSortedKeyBlocks()->empty()) {
        return;
    }
    auto scanner = other->sortState->getScanner(0, skip + limit);
    while (scanner->scan(payloadVecsToScan) > 0) {
        sortState->append(keyVecsToScan, payloadVecsToScan);
    }
    reduce();
}

void TopKBuffer::initVectors() {
    auto payloadState = std::make_shared<common::DataChunkState>();
    auto lastPayloadState = std::make_shared<common::DataChunkState>();
    for (auto& type : orderByDataInfo->payloadTypes) {
        auto payloadVec = std::make_unique<common::ValueVector>(*type, memoryManager);
        auto lastPayloadVec = std::make_unique<common::ValueVector>(*type, memoryManager);
        payloadVec->setState(payloadState);
        lastPayloadVec->setState(lastPayloadState);
        payloadVecsToScan.push_back(payloadVec.get());
        lastPayloadVecsToScan.push_back(lastPayloadVec.get());
        tmpVectors.push_back(std::move(payloadVec));
        tmpVectors.push_back(std::move(lastPayloadVec));
    }
    auto boundaryState = common::DataChunkState::getSingleValueDataChunkState();
    for (auto i = 0u; i < orderByDataInfo->keyTypes.size(); ++i) {
        auto type = orderByDataInfo->keyTypes[i].get();
        auto boundaryVec = std::make_unique<common::ValueVector>(*type, memoryManager);
        boundaryVec->setState(boundaryState);
        boundaryVecs.push_back(std::move(boundaryVec));
        auto posInPayload = orderByDataInfo->keyInPayloadPos[i];
        keyVecsToScan.push_back(payloadVecsToScan[posInPayload]);
        lastKeyVecsToScan.push_back(lastPayloadVecsToScan[posInPayload]);
    }
}

template<typename FUNC>
void TopKBuffer::getSelectComparisonFunction(
    common::PhysicalTypeID typeID, vector_select_comparison_func& selectFunc) {
    switch (typeID) {
    case common::PhysicalTypeID::INT64: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<int64_t, int64_t, FUNC>;
    } break;
    case common::PhysicalTypeID::INT32: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<int32_t, int32_t, FUNC>;
    } break;
    case common::PhysicalTypeID::INT16: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<int16_t, int16_t, FUNC>;
    } break;
    case common::PhysicalTypeID::DOUBLE: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<double_t, double_t, FUNC>;
    } break;
    case common::PhysicalTypeID::FLOAT: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<float_t, float_t, FUNC>;
    } break;
    case common::PhysicalTypeID::BOOL: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<bool, bool, FUNC>;
    } break;
    case common::PhysicalTypeID::STRING: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<common::ku_string_t,
            common::ku_string_t, FUNC>;
    } break;
    case common::PhysicalTypeID::INTERVAL: {
        selectFunc = function::BinaryFunctionExecutor::selectComparison<common::interval_t,
            common::interval_t, FUNC>;
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void TopKBuffer::initCompareFuncs() {
    compareFuncs.reserve(orderByDataInfo->isAscOrder.size());
    equalsFuncs.reserve(orderByDataInfo->isAscOrder.size());
    vector_select_comparison_func compareFunc;
    vector_select_comparison_func equalsFunc;
    for (auto i = 0u; i < orderByDataInfo->isAscOrder.size(); i++) {
        auto physicalType = orderByDataInfo->keyTypes[i]->getPhysicalType();
        if (orderByDataInfo->isAscOrder[i]) {
            getSelectComparisonFunction<function::LessThan>(physicalType, compareFunc);
        } else {
            getSelectComparisonFunction<function::GreaterThan>(physicalType, compareFunc);
        }
        getSelectComparisonFunction<function::Equals>(physicalType, equalsFunc);
        compareFuncs.push_back(compareFunc);
        equalsFuncs.push_back(equalsFunc);
    }
}

void TopKBuffer::setBoundaryValue() {
    for (auto i = 0u; i < boundaryVecs.size(); i++) {
        auto boundaryVec = boundaryVecs[i].get();
        auto dstData =
            boundaryVec->getData() + boundaryVec->getNumBytesPerValue() *
                                         boundaryVec->state->selVector->selectedPositions[0];
        auto srcVector = lastKeyVecsToScan[i];
        auto srcData = srcVector->getData() +
                       srcVector->getNumBytesPerValue() *
                           srcVector->state->selVector
                               ->selectedPositions[srcVector->state->selVector->selectedSize - 1];
        boundaryVec->copyFromVectorData(dstData, srcVector, srcData);
        hasBoundaryValue = true;
    }
}

bool TopKBuffer::compareBoundaryValue(const std::vector<common::ValueVector*>& keyVectors) {
    if (keyVectors[0]->state->isFlat()) {
        return compareFlatKeys(0 /* startKeyVectorIdxToCompare */, keyVectors);
    } else {
        compareUnflatKeys(0 /* startKeyVectorIdxToCompare */, keyVectors);
        return keyVectors[0]->state->selVector->selectedSize > 0;
    }
}

bool TopKBuffer::compareFlatKeys(
    vector_idx_t vectorIdxToCompare, const std::vector<ValueVector*> keyVectors) {
    std::shared_ptr<common::SelectionVector> selVector =
        std::make_shared<common::SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
    selVector->resetSelectorToValuePosBuffer();
    auto compareResult = compareFuncs[vectorIdxToCompare](
        *keyVectors[vectorIdxToCompare], *boundaryVecs[vectorIdxToCompare], *selVector);
    if (vectorIdxToCompare == keyVectors.size() - 1) {
        return compareResult;
    } else if (equalsFuncs[vectorIdxToCompare](*keyVectors[vectorIdxToCompare],
                   *boundaryVecs[vectorIdxToCompare], *selVector)) {
        return compareFlatKeys(vectorIdxToCompare + 1, keyVectors);
    } else {
        return false;
    }
}

void TopKBuffer::compareUnflatKeys(
    vector_idx_t vectorIdxToCompare, const std::vector<ValueVector*> keyVectors) {
    auto compareSelVector =
        std::make_shared<common::SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
    compareSelVector->resetSelectorToValuePosBuffer();
    compareFuncs[vectorIdxToCompare](
        *keyVectors[vectorIdxToCompare], *boundaryVecs[vectorIdxToCompare], *compareSelVector);
    if (vectorIdxToCompare != keyVectors.size() - 1) {
        auto equalsSelVector =
            std::make_shared<common::SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
        equalsSelVector->resetSelectorToValuePosBuffer();
        if (equalsFuncs[vectorIdxToCompare](*keyVectors[vectorIdxToCompare],
                *boundaryVecs[vectorIdxToCompare], *equalsSelVector)) {
            keyVectors[vectorIdxToCompare]->state->selVector = equalsSelVector;
            compareUnflatKeys(vectorIdxToCompare + 1, keyVectors);
            appendSelState(compareSelVector.get(), equalsSelVector.get());
        }
    }
    keyVectors[vectorIdxToCompare]->state->selVector = std::move(compareSelVector);
}

void TopKBuffer::appendSelState(
    common::SelectionVector* selVector, common::SelectionVector* selVectorToAppend) {
    for (auto i = 0u; i < selVectorToAppend->selectedSize; i++) {
        selVector->selectedPositions[selVector->selectedSize + i] =
            selVectorToAppend->selectedPositions[i];
    }
    selVector->selectedSize += selVectorToAppend->selectedSize;
}

void TopKLocalState::init(const OrderByDataInfo& orderByDataInfo,
    storage::MemoryManager* memoryManager, ResultSet& /*resultSet*/, uint64_t skipNumber,
    uint64_t limitNumber) {
    buffer = std::make_unique<TopKBuffer>(orderByDataInfo);
    buffer->init(memoryManager, skipNumber, limitNumber);
}

void TopKLocalState::append(const std::vector<common::ValueVector*>& keyVectors,
    const std::vector<common::ValueVector*>& payloadVectors) {
    buffer->append(keyVectors, payloadVectors);
    buffer->reduce();
}

void TopK::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState = std::make_unique<TopKLocalState>();
    localState->init(*info, context->memoryManager, *resultSet, skipNumber, limitNumber);
    for (auto& dataPos : info->payloadsPos) {
        payloadVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
    for (auto& dataPos : info->keysPos) {
        orderByVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
}

void TopK::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < resultSet->multiplicity; i++) {
            localState->append(orderByVectors, payloadVectors);
        }
    }
    localState->finalize();
    sharedState->mergeLocalState(localState.get());
}

} // namespace processor
} // namespace kuzu
