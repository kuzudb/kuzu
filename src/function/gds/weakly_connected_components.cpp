#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;

namespace kuzu {
namespace function {

class WeaklyConnectedComponentLocalState : public GDSLocalState {
public:
    explicit WeaklyConnectedComponentLocalState(main::ClientContext* context) {
        auto mm = context->getMemoryManager();
        nodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        groupVector = std::make_unique<ValueVector>(*LogicalType::INT64(), mm);
        nodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        groupVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(nodeIDVector.get());
        vectors.push_back(groupVector.get());
    }

    void materialize(graph::Graph* graph, const std::vector<int64_t>& groupArray,
        FactorizedTable& table) const {
        for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
            nodeIDVector->setValue<nodeID_t>(0, {offset, graph->getNodeTableID()});
            groupVector->setValue<int64_t>(0, groupArray[offset]);
            table.append(vectors);
        }
    }

private:
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> groupVector;
    std::vector<ValueVector*> vectors;
};

class WeaklyConnectedComponent final : public GDSAlgorithm {
public:
    WeaklyConnectedComponent() = default;
    WeaklyConnectedComponent(const WeaklyConnectedComponent& other) : GDSAlgorithm{other} {}

    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return std::vector<LogicalTypeID>{LogicalTypeID::ANY};
    }

    std::vector<std::string> getResultColumnNames() const override {
        return {"node_id", "group_id"};
    }

    std::vector<common::LogicalType> getResultColumnTypes() const override {
        return {*LogicalType::INTERNAL_ID(), *LogicalType::INT64()};
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<WeaklyConnectedComponentLocalState>(context);
    }

    void exec() override {
        auto wccLocalState = localState->ptrCast<WeaklyConnectedComponentLocalState>();
        visitedArray.resize(graph->getNumNodes());
        groupArray.resize(graph->getNumNodes());
        for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
            visitedArray[offset] = false;
            groupArray[offset] = -1;
        }
        auto groupID = 0;
        for (auto offset = 0u; offset < graph->getNumNodes(); ++offset) {
            if (visitedArray[offset]) {
                continue;
            }
            findConnectedComponent(offset, groupID++);
        }
        wccLocalState->materialize(graph, groupArray, *table);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<WeaklyConnectedComponent>(*this);
    }

private:
    void findConnectedComponent(common::offset_t offset, int64_t groupID) {
        visitedArray[offset] = true;
        groupArray[offset] = groupID;
        auto nbrs = graph->getNbrs(offset);
        for (auto nbr : nbrs) {
            if (visitedArray[nbr.offset]) {
                continue;
            }
            findConnectedComponent(nbr.offset, groupID);
        }
    }

private:
    std::vector<bool> visitedArray;
    std::vector<int64_t> groupArray;
};

function_set WeaklyConnectedComponentsFunction::getFunctionSet() {
    function_set result;
    auto function =
        std::make_unique<GDSFunction>(name, std::make_unique<WeaklyConnectedComponent>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
