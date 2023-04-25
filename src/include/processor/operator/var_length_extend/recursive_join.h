//#pragma once
//
//#include "processor/operator/sink.h"
//#include "processor/operator/var_length_extend/bfs_state.h"
//
//namespace kuzu {
//namespace processor {
//
//class RecursiveJoin : public Sink {
//public:
//    RecursiveJoin(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
//        std::shared_ptr<FTableSharedState> globalSharedState,
//        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
//        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::RECURSIVE_JOIN,
//              std::move(child), id, paramsString},
//          globalSharedState{std::move(globalSharedState)} {}
//
//    inline void setThreadLocalSharedState(std::shared_ptr<SSPThreadLocalSharedState> state) {
//        threadLocalSharedState = std::move(state);
//    }
//
//    void executeInternal(ExecutionContext* context) override;
//
//    inline std::unique_ptr<PhysicalOperator> clone() override {
//        return std::make_unique<RecursiveJoin>(
//            resultSetDescriptor->copy(), globalSharedState, children[0]->clone(), id, paramsString);
//    }
//
//private:
//    void updateVisitedState();
//
//private:
//    std::shared_ptr<FTableSharedState> globalSharedState;
//    std::shared_ptr<SSPThreadLocalSharedState> threadLocalSharedState;
//};
//
//} // namespace processor
//} // namespace kuzu