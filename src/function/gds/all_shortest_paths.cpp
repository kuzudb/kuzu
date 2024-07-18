//#include "common/exception/runtime.h"
//#include "function/gds/gds_frontier.h"
//#include "function/gds/gds_function_collection.h"
//#include "function/gds/rec_joins.h"
//#include "function/gds_function.h"
//#include "processor/operator/gds_call.h"
//#include "processor/result/factorized_table.h"
//
//// TODO(Semih): Remove
//#include <iostream>
//using namespace kuzu::processor;
//using namespace kuzu::common;
//using namespace kuzu::binder;
//using namespace kuzu::storage;
//using namespace kuzu::graph;
//
//namespace kuzu {
//namespace function {
//
//
//class SingleNodeTableMultiplicities {
//public:
//    explicit SingleNodeTableMultiplicities(std::atomic<uint64_t>* currNodeTableMultiplicities)
//        : multiplicities{currNodeTableMultiplicities} {}
//
//    void incrementMultiplicity(nodeID_t nodeID, uint64_t multiplicity) {
//        multiplicities[nodeID.offset].fetch_add(multiplicity);
//    }
//
//    uint64_t getMultiplicity(offset_t nodeOffset) {
//        return multiplicities[nodeOffset].load();
//    }
//private:
//    std::atomic<uint64_t>* multiplicities;
//};
//
///**
// * A dense storage structures for multiplicities for multiple node tables. Users of this class
// * should call fixNodeTable to fix one node table first. And then call
// * getCurrNodeTableMultiplicities, which returns back a unique ptr of SingleNodeTableMultiplicities.
// * Then reads and writes should be made through SingleNodeTableMultiplicities.
// *
// * The goal of this rather complicated design is the following: We want to keep a pointer
// * to the block that keeps one table's multiplicities, so we don't have to do a map look up
// * each time we want to read a multiplicity or a write multiplicity. We store the tableID that is
// * currently fixed in the currNodeTableID field, which is an atomic<table_id_t> and the
// * multiplicities pointer in SingleNodeTableMultiplicities. Since each worker thread will get
// * and create new SingleNodeTableMultiplicities, each thread will have their own multiplicities
// * pointer and through that can call SingleNodeTableMultiplicities::getMultiplicity() and
// * SingleNodeTableMultiplicities::incrementMultiplicity() functions to read & update multiplicities
// * in a thread-safe manner.
// *
// * Note that alternatively we can keep a single multiplicities pointer in
// * ConcurrentPathMultiplicities that we update atomically as we set it to different node tables.
// * However, this would mean that each time we access a multiplicity, we would have to do as follows:
// * multiplicities.load()[i].load(), instead of what is currently done in
// * SingleNodeTableMultiplicities, which has a single load() call: multiplicities[i].load().
// * TODO(Semih): Test the performance difference of this design.
// */
//class ConcurrentPathMultiplicities {
//public:
//    explicit ConcurrentPathMultiplicities(std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
//        MemoryManager* mm)  {
//        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
//            auto memoryBuffer = mm->allocateBuffer(false, numNodes * sizeof(std::atomic<uint64_t>));
//            auto multiplicitiesPtr = reinterpret_cast<std::atomic<uint64_t>*>(memoryBuffer->buffer.data());
//            for (uint64_t i = 0; i < numNodes; ++i) {
//                multiplicitiesPtr[i].store(0);
//            }
//            multiplicitiesMap.insert({tableID, move(memoryBuffer)});
//        }
//    }
//
//    std::unique_ptr<SingleNodeTableMultiplicities> getCurrNodeTableMultiplicities() {
//        KU_ASSERT(multiplicitiesMap.contains(currNodeTableID.load()));
//        return std::make_unique<SingleNodeTableMultiplicities>(reinterpret_cast<std::atomic<uint64_t>*>(
//            multiplicitiesMap.at(currNodeTableID.load()).get()->buffer.data()));
//    }
//
//    void fixNodeTable(common::table_id_t tableID) {
//        KU_ASSERT(multiplicitiesMap.contains(tableID));
//        currNodeTableID.store(tableID);
//    }
//private:
//    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> multiplicitiesMap;
//    // currNodeTableID is the ID of current table whose multiplicities will be updated. Since this
//    // class is designed to be used in multi-threaded execution, currNodeTableID is an atomic
//    // itself, and we change it and read it atomically in the fixNodeTable and
//    // getCurrNodeTableMultiplicities functions.
//    std::atomic<table_id_t> currNodeTableID;
//};
//
///**
// * A data structure to keep track of all (shortest) path from one source node to
// * multiple destination (and intermediate) nodes as "backward BFS graph" form. The data structure is
// * "dense" in the sense that it keeps space for a pointer to the "backwards edges" for each
// * possible node that can be in the destination. Supports multi-label nodes.
// */
//class AllPaths {
//public:
//    explicit AllPaths(
//        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes,
//        MemoryManager* mm) {
//        for (const auto& [tableID, numNodes] : nodeTableIDAndNumNodes) {
//            lastEdges.insert({tableID, mm->allocateBuffer(false, numNodes * sizeof(nodeID_t))});
//        }
//    }
//
//    void setLastEdge(nodeID_t nodeID, nodeID_t lastEdge) {
//        KU_ASSERT(currentFixedLastEdges != nullptr);
//        currentFixedLastEdges[nodeID.offset] = lastEdge;
//    }
//
//    nodeID_t getParent(nodeID_t nodeID) {
//        return reinterpret_cast<nodeID_t*>(
//            lastEdges.at(nodeID.tableID).get()->buffer.data())[nodeID.offset];
//    }
//
//    void fixNodeTable(common::table_id_t tableID) {
//        KU_ASSERT(lastEdges.contains(tableID));
//        currentFixedLastEdges =
//            reinterpret_cast<nodeID_t*>(lastEdges.at(tableID).get()->buffer.data());
//    }
//
//private:
//    common::table_id_map_t<std::unique_ptr<MemoryBuffer>> lastEdges;
//    nodeID_t* currentFixedLastEdges;
//};
//
//class AllSPPathsFrontiers : public PathLengthsFrontiers {
//    friend struct AllSPPathsFrontierCompute;
//
//public:
//    explicit AllSPPathsFrontiers(PathLengths* pathLengths, AllPaths* allPaths)
//        : PathLengthsFrontiers(pathLengths), allPaths{allPaths} {}
//    void beginFrontierComputeBetweenTables(
//        table_id_t curFrontierTableID, table_id_t nextFrontierTableID) override {
//        PathLengthsFrontiers::beginFrontierComputeBetweenTables(
//            curFrontierTableID, nextFrontierTableID);
//        allPaths->fixNodeTable(nextFrontierTableID);
//    }
//
//private:
//    AllPaths* allPaths;
//};
//
//struct AllSPOutputs : public RJOutputs {
//    nodeID_t sourceNodeID;
//    std::unique_ptr<PathLengths> pathLengths;
//    std::unique_ptr<ConcurrentPathMultiplicities> pathMultiplicities;
//    explicit AllSPOutputs(graph::Graph* graph, nodeID_t sourceNodeID, RJOutputType outputType, MemoryManager* mm = nullptr)
//        : sourceNodeID{sourceNodeID} {
//        std::vector<std::tuple<common::table_id_t, uint64_t>> nodeTableIDAndNumNodes;
//        for (common::table_id_t tableID : graph->getNodeTableIDs()) {
//            auto numNodes = graph->getNumNodes(tableID);
//            nodeTableIDAndNumNodes.push_back({tableID, numNodes});
//        }
//        pathLengths = std::make_unique<PathLengths>(nodeTableIDAndNumNodes);
//        pathMultiplicities = std::make_unique<ConcurrentPathMultiplicities>(nodeTableIDAndNumNodes, mm);
//    }
//};
//
//class AllSPOutputWriter : public RJOutputWriter {
//public:
//    explicit AllSPOutputWriter(main::ClientContext* context, RJOutputType outputType)
//        : RJOutputWriter(context, outputType) {}
//    void materialize(graph::Graph* graph, RJOutputs* rjOutputs,
//        processor::FactorizedTable& fTable) const override {
//        auto spOutputs = rjOutputs->ptrCast<AllSPOutputs>();
//        srcNodeIDVector->setValue<nodeID_t>(0, spOutputs->sourceNodeID);
//        for (auto tableID : graph->getNodeTableIDs()) {
//            spOutputs->pathMultiplicities->fixNodeTable(tableID);
//            auto fixedNodeTableMultiplicites =
//                spOutputs->pathMultiplicities->getCurrNodeTableMultiplicities();
//            spOutputs->pathLengths->fixCurFrontierNodeTable(tableID);
//            for (offset_t nodeOffset = 0;
//                 nodeOffset < spOutputs->pathLengths->getNumNodesInCurFrontierFixedNodeTable();
//                 ++nodeOffset) {
//                auto length =
//                    spOutputs->pathLengths->getMaskValueFromCurFrontierFixedMask(nodeOffset);
//                if (length == PathLengths::UNVISITED) {
//                    continue;
//                }
//                for (uint64_t i = 0; i < fixedNodeTableMultiplicites->getMultiplicity(nodeOffset); ++i) {
//                    auto dstNodeID = nodeID_t{nodeOffset, tableID};
//                    dstNodeIDVector->setValue<nodeID_t>(0, dstNodeID);
//                    if (RJOutputType::LENGTHS == rjOutputType ||
//                        RJOutputType::PATHS == rjOutputType) {
//                        lengthVector->setValue<int64_t>(0, length);
//                    }
//                    fTable.append(vectors);
//                }
//            }
//        }
//    }
//};
//
//struct AllSPLengthsFrontierCompute : public FrontierCompute {
//    explicit AllSPLengthsFrontierCompute(PathLengthsFrontiers* pathLengthsFrontiers, ConcurrentPathMultiplicities* multiplicities)
//        : pathLengthsFrontiers{pathLengthsFrontiers}, multiplicities{multiplicities} {};
//
//    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
//        auto retVal = pathLengthsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
//                   nbrID.offset) == PathLengths::UNVISITED;
//        if (retVal) {
//            fixedTableMultiplicities->incrementMultiplicity(nbrID,
//                fixedTableMultiplicities->getMultiplicity(curNodeID.offset));
//        }
//        return retVal;
//    }
//
//    void initFrontierExtensions(table_id_t toNodeTableID) override {
//        multiplicities->fixNodeTable(toNodeTableID);
//    };
//
//    void init() override {
//        fixedTableMultiplicities = multiplicities->getCurrNodeTableMultiplicities();
//    }
//
//    std::unique_ptr<FrontierCompute> clone() override {
//        return std::make_unique<AllSPLengthsFrontierCompute>(
//            this->pathLengthsFrontiers, this->multiplicities);
//    }
//private:
//    PathLengthsFrontiers* pathLengthsFrontiers;
//    ConcurrentPathMultiplicities* multiplicities;
//    std::unique_ptr<SingleNodeTableMultiplicities> fixedTableMultiplicities;
//};
//
//struct AllSPPathsFrontierCompute : public FrontierCompute {
//    AllSPPathsFrontiers* allSPPathsFrontiers;
//    explicit AllSPPathsFrontierCompute(AllSPPathsFrontiers* allPathsFrontiers)
//        : allSPPathsFrontiers{allPathsFrontiers} {};
//
//    bool edgeCompute(nodeID_t curNodeID, nodeID_t nbrID) override {
//        auto retVal = allSPPathsFrontiers->pathLengths->getMaskValueFromNextFrontierFixedMask(
//                          nbrID.offset) == PathLengths::UNVISITED;
//        if (retVal) {
//            // We set the nbrID's last edge to curNodeID;
//            allSPPathsFrontiers->allPaths->setLastEdge(nbrID, curNodeID);
//        }
//        return retVal;
//    }
//};
//
///**
// * Algorithm for parallel single shortest path computation, i.e., assumes Distinct semantics, so
// * one arbitrary shortest path is returned for each destination. If paths are not returned,
// * multiplicities of each destination is ignored (e.g., if there are 3 paths to a destination d,
// * d is returned only one).
// */
//class AllSPAlgorithm final : public RJAlgorithm {
//
//public:
//    explicit AllSPAlgorithm(RJOutputType outputType) : RJAlgorithm(outputType) {};
//    AllSPAlgorithm(const AllSPAlgorithm& other) : RJAlgorithm(other) {}
//
//    std::unique_ptr<GDSAlgorithm> copy() const override {
//        return std::make_unique<AllSPAlgorithm>(*this);
//    }
//
//protected:
//    void initLocalState(main::ClientContext* context) override {
//        outputWriter = std::make_unique<AllSPOutputWriter>(context, outputType);
//    }
//
//    std::unique_ptr<RJCompState> getFrontiersAndFrontiersCompute(processor::ExecutionContext* executionContext,
//        nodeID_t sourceNodeID) override {
//        auto spOutputs = std::make_unique<AllSPOutputs>(sharedState->graph.get(), sourceNodeID,
//            outputType, executionContext->clientContext->getMemoryManager());
//        switch (outputType) {
//        case RJOutputType::DESTINATION_NODES:
//        case RJOutputType::LENGTHS: {
//            auto pathLengthsFrontiers =
//                std::make_unique<PathLengthsFrontiers>(spOutputs->pathLengths.get());
//            auto frontierCompute = std::make_unique<AllSPLengthsFrontierCompute>(
//                pathLengthsFrontiers.get(), spOutputs->pathMultiplicities.get());
//            return std::make_unique<RJCompState>(move(spOutputs), move(pathLengthsFrontiers), move(frontierCompute));
//        }
////        case RJOutputType::PATHS: {
////            auto singlePathsFrontiers = std::make_unique<AllSPPathsFrontiers>(
////                sourceState->pathLengths.get(), sourceState->singlePaths.get());
////            auto frontierCompute =
////                std::make_unique<AllSPPathsFrontierCompute>(singlePathsFrontiers.get());
////            return std::make_unique<RJCompState>(move(sourceState), move(singlePathsFrontiers), move(frontierCompute));
////        }
//        default:
//            throw RuntimeException("Unrecognized RJOutputType in "
//                                   "RJAlgorithm::getFrontiersAndFrontiersCompute(): " +
//                                   std::to_string(static_cast<uint8_t>(outputType)) + ".");
//        }
//    }
//};
//
//function_set AllSPDestinationsFunction::getFunctionSet() {
//    function_set result;
//    auto function = std::make_unique<GDSFunction>(
//        name, std::make_unique<AllSPAlgorithm>(RJOutputType::DESTINATION_NODES));
//    result.push_back(std::move(function));
//    return result;
//}
//
//function_set AllSPLengthsFunction::getFunctionSet() {
//    function_set result;
//    auto function = std::make_unique<GDSFunction>(
//        name, std::make_unique<AllSPAlgorithm>(RJOutputType::LENGTHS));
//    result.push_back(std::move(function));
//    return result;
//}
//
//function_set AllSPPathsFunction::getFunctionSet() {
//    function_set result;
//    auto function = std::make_unique<GDSFunction>(
//        name, std::make_unique<AllSPAlgorithm>(RJOutputType::PATHS));
//    result.push_back(std::move(function));
//    return result;
//}
//
//} // namespace function
//} // namespace kuzu
