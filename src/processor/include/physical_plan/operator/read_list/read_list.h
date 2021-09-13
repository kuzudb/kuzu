#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/storage/include/data_structure/lists/lists.h"

namespace graphflow {
namespace processor {

class ReadList : public PhysicalOperator {

public:
    ReadList(const DataPos& inDataPos, const DataPos& outDataPos, Lists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id,
        bool isAdjList)
        : PhysicalOperator{move(prevOperator), READ_LIST, context, id}, inDataPos{inDataPos},
          outDataPos{outDataPos}, lists{lists}, largeListHandle{
                                                    make_unique<LargeListHandle>(isAdjList)} {}

    ~ReadList() override{};

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void printMetricsToJson(nlohmann::json& json, Profiler& profiler) override;

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
