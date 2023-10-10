#include "processor/operator/persistent/copy_to_parquet.h"

namespace kuzu {
namespace processor {

void CopyToParquetLocalState::init(
    CopyToParquetInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) {
    ft = std::make_unique<FactorizedTable>(mm, std::move(info->tableSchema));
    vectorsToAppend.reserve(info->dataPoses.size());
    for (auto& pos : info->dataPoses) {
        vectorsToAppend.push_back(resultSet->getValueVector(pos).get());
    }
    this->mm = mm;
}

std::vector<std::unique_ptr<common::LogicalType>> CopyToParquetInfo::copyTypes() {
    std::vector<std::unique_ptr<common::LogicalType>> copiedTypes;
    for (auto& type : types) {
        copiedTypes.push_back(type->copy());
    }
    return copiedTypes;
}

void CopyToParquet::initLocalStateInternal(
    kuzu::processor::ResultSet* resultSet, kuzu::processor::ExecutionContext* context) {
    localState->init(info.get(), context->memoryManager, resultSet);
}

void CopyToParquet::executeInternal(kuzu::processor::ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        localState->append();
        if (localState->ft->getTotalNumFlatTuples() > common::StorageConstants::NODE_GROUP_SIZE) {
            sharedState->writer->flush(*localState->ft);
        }
    }
    sharedState->writer->flush(*localState->ft);
}

} // namespace processor
} // namespace kuzu
