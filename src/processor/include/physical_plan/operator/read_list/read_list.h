#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace processor {

class ReadList : public PhysicalOperator {

public:
    ReadList(const DataPos& inDataPos, const DataPos& outDataPos, Lists* lists,
        unique_ptr<PhysicalOperator> child, uint32_t id, bool isAdjList, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, inDataPos{inDataPos},
          outDataPos{outDataPos}, lists{lists}, largeListHandle{
                                                    make_unique<LargeListHandle>(isAdjList)} {}

    ~ReadList() override{};

    PhysicalOperatorType getOperatorType() override = 0;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void reInitToRerunSubPlan() override;

protected:
    void readValuesFromList();

protected:
    DataPos inDataPos;
    DataPos outDataPos;

    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<ValueVector> inValueVector;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;

    Lists* lists;
    unique_ptr<LargeListHandle> largeListHandle;
};

} // namespace processor
} // namespace graphflow
