//#include "processor/operator/var_length_extend/recursive_join.h"
//
//namespace kuzu {
//namespace processor {
//
//void RecursiveJoin::executeInternal(ExecutionContext* context) {
//    while (children[0]->getNextTuple(context)) {
//        updateVisitedState();
//    }
//    globalSharedState->mergeLocalTable(*threadLocalSharedState->fTable);
//}
//
//void RecursiveJoin::updateVisitedState() {
//    auto visitedNodes = threadLocalSharedState->sspMorsel->visitedNodes;
//    auto nodeIDVector = threadLocalSharedState->tmpDstNodeIDVector.get();
//    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
//        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
//        auto nodeID = nodeIDVector->getValue<common::nodeID_t>(pos);
//        if (visitedNodes[nodeID.offset] == VisitedState::NOT_VISITED) {
//            threadLocalSharedState->sspMorsel->markOnVisit(nodeID.offset);
//            continue;
//        }
//    }
//}
//
//} // namespace processor
//} // namespace kuzu
