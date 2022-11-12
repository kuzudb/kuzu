#pragma once

#include "src/processor/operator/include/physical_operator.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace kuzu {
namespace processor {

class ScanList : public PhysicalOperator {

public:
    ScanList(const DataPos& inDataPos, const DataPos& outDataPos,
        ListsWithAdjAndPropertyListsUpdateStore* listsWithAdjAndPropertyListsUpdateStore,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, inDataPos{inDataPos},
          outDataPos{outDataPos}, listsWithAdjAndPropertyListsUpdateStore{
                                      listsWithAdjAndPropertyListsUpdateStore} {}

    ~ScanList() override{};

    PhysicalOperatorType getOperatorType() override = 0;

    inline shared_ptr<ResultSet> init(ExecutionContext* context) override {
        resultSet = PhysicalOperator::init(context);
        inDataChunk = resultSet->dataChunks[inDataPos.dataChunkPos];
        inValueVector = inDataChunk->valueVectors[inDataPos.valueVectorPos];
        outDataChunk = resultSet->dataChunks[outDataPos.dataChunkPos];
        return resultSet;
    }

protected:
    DataPos inDataPos;
    DataPos outDataPos;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<ValueVector> inValueVector;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;

    ListsWithAdjAndPropertyListsUpdateStore* listsWithAdjAndPropertyListsUpdateStore;
    shared_ptr<ListHandle> listHandle;
};

} // namespace processor
} // namespace kuzu
