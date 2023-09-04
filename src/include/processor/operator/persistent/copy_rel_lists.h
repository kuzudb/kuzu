#pragma once

#include "processor/operator/persistent/copy_rel.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

class CopyRelLists : public CopyRel {
public:
    CopyRelLists(CopyRelInfo info, std::shared_ptr<CopyRelSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> left, std::unique_ptr<PhysicalOperator> right,
        uint32_t id, const std::string& paramsString)
        : CopyRel{std::move(info), std::move(sharedState), std::move(resultSetDescriptor),
              PhysicalOperatorType::COPY_REL_LISTS, std::move(left), std::move(right), id,
              paramsString} {}

    // Used for clone only.
    CopyRelLists(CopyRelInfo info, std::shared_ptr<CopyRelSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> left, uint32_t id, const std::string& paramsString)
        : CopyRel{std::move(info), std::move(sharedState), std::move(resultSetDescriptor),
              PhysicalOperatorType::COPY_REL_LISTS, std::move(left), id, paramsString} {}

    void executeInternal(ExecutionContext* context) final;
    void finalize(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyRelLists>(
            info, sharedState, resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

private:
    void copyRelLists(DirectedInMemRelData* relData, const DataPos& boundOffsetPos,
        const DataPos& adjOffsetPos, const std::vector<DataPos>& dataPoses,
        std::vector<std::unique_ptr<storage::PropertyCopyState>>& copyStates);
};

} // namespace processor
} // namespace kuzu
