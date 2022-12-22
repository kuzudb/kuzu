#pragma once

#include <utility>

#include "binder/expression/expression.h"
#include "common/vector/value_vector_utils.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/base_shortest_path.h"
#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class ShortestPathAdjList : public BaseShortestPath {
public:
    ShortestPathAdjList(const DataPos& srcDataPos, const DataPos& destDataPos, AdjLists* adjList,
        Lists* relPropertyLists, uint64_t lowerBound, uint64_t upperBound,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseShortestPath{PhysicalOperatorType::SHORTEST_PATH_ADJ_LIST, srcDataPos, destDataPos,
              lowerBound, upperBound, move(child), id, paramsString},
          listSyncState{make_shared<ListSyncState>()}, listHandle{make_shared<ListHandle>(
                                                           *listSyncState)},
          lists{adjList}, relPropertyLists{move(relPropertyLists)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    bool computeShortestPath(uint64_t currIdx, uint64_t destIdx);

    bool addToNextFrontier(node_offset_t parentNodeOffset, node_offset_t destNodeOffset);

    bool getNextBatchOfChildNodes();

    bool extendToNextFrontier(node_offset_t destNodeOffset);

    void printShortestPath(node_offset_t destNodeOffset);

    void resetFrontier();

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ShortestPathAdjList>(srcDataPos, destDataPos, lists, relPropertyLists,
            lowerBound, upperBound, children[0]->clone(), id, paramsString);
    }

private:
    shared_ptr<ValueVector> adjNodeIDVector;
    shared_ptr<ValueVector> relIDVector;
    shared_ptr<ListSyncState> listSyncState;
    shared_ptr<ListHandle> listHandle;
    AdjLists* lists;
    Lists* relPropertyLists;
    shared_ptr<ListHandle> relListHandles;
};

} // namespace processor
} // namespace kuzu
