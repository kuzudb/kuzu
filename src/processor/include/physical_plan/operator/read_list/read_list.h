#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace processor {

class ReadList : public PhysicalOperator {

public:
    ReadList(const DataPos& inDataPos, const DataPos& outDataPos,
        ListsWithAdjAndPropertyListsUpdateStore* listsWithAdjAndPropertyListsUpdateStore,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, inDataPos{inDataPos},
          outDataPos{outDataPos}, listsWithAdjAndPropertyListsUpdateStore{
                                      listsWithAdjAndPropertyListsUpdateStore} {}

    ~ReadList() override{};

    PhysicalOperatorType getOperatorType() override = 0;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

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
} // namespace graphflow
