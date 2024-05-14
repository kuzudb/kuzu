#include "processor/operator/persistent/copy_to_parquet.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

void CopyToParquetLocalState::init(CopyToInfo* info, storage::MemoryManager* mm,
    ResultSet* resultSet) {
    auto copyToInfo = info->constPtrCast<CopyToParquetInfo>();
    ft = std::make_unique<FactorizedTable>(mm, copyToInfo->tableSchema.copy());
    numTuplesInFT = 0;
    countingVec = nullptr;
    vectorsToAppend.reserve(info->dataPoses.size());
    countingVec = resultSet->getValueVector(copyToInfo->countingVecPos).get();
    for (auto& pos : info->dataPoses) {
        vectorsToAppend.push_back(resultSet->getValueVector(pos).get());
    }
    this->mm = mm;
}

void CopyToParquetLocalState::sink(CopyToSharedState* sharedState, CopyToInfo* /*info*/) {
    ft->append(vectorsToAppend);
    numTuplesInFT += countingVec->state->getSelVector().getSelSize();
    if (numTuplesInFT > StorageConstants::NODE_GROUP_SIZE) {
        reinterpret_cast<CopyToParquetSharedState*>(sharedState)->flush(*ft);
        numTuplesInFT = 0;
    }
}

void CopyToParquetLocalState::finalize(CopyToSharedState* sharedState) {
    reinterpret_cast<CopyToParquetSharedState*>(sharedState)->flush(*ft);
}

void CopyToParquetSharedState::init(CopyToInfo* info, main::ClientContext* context) {
    auto parquetInfo = reinterpret_cast<CopyToParquetInfo*>(info);
    writer = std::make_unique<ParquetWriter>(parquetInfo->fileName,
        LogicalType::copy(parquetInfo->types), parquetInfo->names, parquetInfo->codec, context);
}

void CopyToParquetSharedState::finalize() {
    writer->finalize();
}

void CopyToParquetSharedState::flush(FactorizedTable& ft) {
    writer->flush(ft);
}

} // namespace processor
} // namespace kuzu
