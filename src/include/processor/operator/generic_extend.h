#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

class GenericExtend : public PhysicalOperator {
public:
    GenericExtend(const DataPos& inVectorPos, const DataPos& outVectorPos, vector<Column*> columns,
        vector<Lists*> lists, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{std::move(child), id, paramsString}, inVectorPos{inVectorPos},
          outVectorPos{outVectorPos}, columns{std::move(columns)}, lists{std::move(lists)} {}
    ~GenericExtend() override = default;

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::GENERIC_EXTEND;
    }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<GenericExtend>(
            inVectorPos, outVectorPos, columns, lists, children[0]->clone(), id, paramsString);
    }

private:
    bool findOutput();

    inline bool hasColumnToScan() { return nextColumnIdx < columns.size(); }
    inline bool hasListToScan() { return nextListIdx < lists.size(); }
    bool findInColumns();
    bool findInLists();
    bool scanColumn(uint32_t idx);
    bool scanList(uint32_t idx);

private:
    DataPos inVectorPos;
    DataPos outVectorPos;
    vector<Column*> columns;
    vector<Lists*> lists;
    vector<shared_ptr<ListHandle>> listHandles;

    shared_ptr<ValueVector> inVector;
    shared_ptr<ValueVector> outVector;
    uint32_t nextColumnIdx;
    uint32_t nextListIdx;
    // A list may be scanned for multi getNext() call e.g. large list. So we track current list.
    Lists* currentList;
    ListHandle* currentListHandle;
    node_offset_t currentNodeOffset;
};

} // namespace processor
} // namespace kuzu
