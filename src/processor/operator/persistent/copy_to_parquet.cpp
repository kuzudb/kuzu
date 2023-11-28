#include "processor/operator/persistent/copy_to_parquet.h"

#include <memory>
#include <utility>

#include "common/constants.h"
#include "common/types/types.h"
#include "processor/operator/persistent/copy_to.h"
#include "processor/operator/persistent/writer/parquet/parquet_writer.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace processor {

using namespace kuzu::common;
using namespace kuzu::storage;

void CopyToParquetLocalState::init(
    CopyToInfo* info, storage::MemoryManager* mm, ResultSet* resultSet) {
    auto copyToInfo = reinterpret_cast<CopyToParquetInfo*>(info);
    ft = std::make_unique<FactorizedTable>(mm, std::move(copyToInfo->tableSchema));
    vectorsToAppend.reserve(info->dataPoses.size());
    for (auto& pos : info->dataPoses) {
        vectorsToAppend.push_back(resultSet->getValueVector(pos).get());
    }
    this->mm = mm;
}

void CopyToParquetLocalState::sink(CopyToSharedState* sharedState, CopyToInfo* /*info*/) {
    ft->append(vectorsToAppend);
    if (ft->getTotalNumFlatTuples() > StorageConstants::NODE_GROUP_SIZE) {
        reinterpret_cast<CopyToParquetSharedState*>(sharedState)->flush(*ft);
    }
}

void CopyToParquetLocalState::finalize(CopyToSharedState* sharedState) {
    reinterpret_cast<CopyToParquetSharedState*>(sharedState)->flush(*ft);
}

void CopyToParquetSharedState::init(CopyToInfo* info, MemoryManager* mm) {
    auto parquetInfo = reinterpret_cast<CopyToParquetInfo*>(info);
    writer = std::make_unique<ParquetWriter>(parquetInfo->fileName,
        LogicalType::copy(parquetInfo->types), parquetInfo->names, parquetInfo->codec, mm);
}

void CopyToParquetSharedState::finalize() {
    writer->finalize();
}

void CopyToParquetSharedState::flush(FactorizedTable& ft) {
    writer->flush(ft);
}

} // namespace processor
} // namespace kuzu
