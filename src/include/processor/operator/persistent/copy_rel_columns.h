#pragma once

#include "processor/operator/persistent/copy_rel.h"
#include "processor/operator/sink.h"
#include "storage/index/hash_index.h"
#include "storage/store/rels_statistics.h"

namespace kuzu {
namespace processor {

class CopyRelColumns : public CopyRel {
public:
    CopyRelColumns(CopyRelInfo info, std::shared_ptr<CopyRelSharedState> sharedState,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : CopyRel{std::move(info), std::move(sharedState), std::move(resultSetDescriptor),
              PhysicalOperatorType::COPY_REL_COLUMNS, std::move(child), id, paramsString} {}

    void executeInternal(ExecutionContext* context) final;
    void finalize(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyRelColumns>(
            info, sharedState, resultSetDescriptor->copy(), children[0]->clone(), id, paramsString);
    }

private:
    common::row_idx_t copyRelColumns(common::RelDataDirection relDirection,
        DirectedInMemRelData* relData, const CopyRelInfo& info,
        std::vector<std::unique_ptr<storage::PropertyCopyState>>& copyStates);
    common::row_idx_t countRelLists(DirectedInMemRelData* relData, const DataPos& boundOffsetPos);
    void checkViolationOfUniqueness(common::RelDataDirection relDirection,
        arrow::Array* boundNodeOffsets, storage::InMemColumnChunk* adjColumnChunk) const;

    static void buildRelListsHeaders(storage::ListHeadersBuilder* relListHeadersBuilder,
        const storage::atomic_uint64_vec_t& relListsSizes);
    static void buildRelListsMetadata(DirectedInMemRelData* directedInMemRelData);
    static void buildRelListsMetadata(
        storage::InMemLists* relLists, storage::ListHeadersBuilder* relListHeaders);
    static void flushRelColumns(DirectedInMemRelData* directedInMemRelData);
};

} // namespace processor
} // namespace kuzu
