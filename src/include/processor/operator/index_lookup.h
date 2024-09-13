#pragma once

#include "processor/operator/persistent/batch_insert_error_handler.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace storage {
class NodeTable;
} // namespace storage
namespace processor {

struct BatchInsertSharedState;
struct IndexLookupInfo {
    storage::NodeTable* nodeTable;
    // In copy rdf, we need to perform lookup before primary key is persist on disk. So we need to
    // use index builder.
    std::shared_ptr<BatchInsertSharedState> batchInsertSharedState;
    DataPos keyVectorPos;
    DataPos resultVectorPos;
    std::vector<DataPos> warningDataPos;

    IndexLookupInfo(storage::NodeTable* nodeTable, const DataPos& keyVectorPos,
        const DataPos& resultVectorPos, std::vector<DataPos> warningDataPos)
        : nodeTable{nodeTable}, batchInsertSharedState{nullptr}, keyVectorPos{keyVectorPos},
          resultVectorPos{resultVectorPos}, warningDataPos(std::move(warningDataPos)) {}
    IndexLookupInfo(const IndexLookupInfo& other) = default;

    inline std::unique_ptr<IndexLookupInfo> copy() {
        return std::make_unique<IndexLookupInfo>(*this);
    }
};

struct IndexLookupPrintInfo final : OPPrintInfo {
    binder::expression_vector expressions;
    explicit IndexLookupPrintInfo(binder::expression_vector expressions)
        : expressions{std::move(expressions)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<IndexLookupPrintInfo>(new IndexLookupPrintInfo(*this));
    }

private:
    IndexLookupPrintInfo(const IndexLookupPrintInfo& other)
        : OPPrintInfo{other}, expressions{other.expressions} {}
};

struct IndexLookupLocalState {
    explicit IndexLookupLocalState(std::unique_ptr<BatchInsertErrorHandler> errorHandler)
        : errorHandler(std::move(errorHandler)) {}

    std::unique_ptr<BatchInsertErrorHandler> errorHandler;
};

class IndexLookup : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::INDEX_LOOKUP;

public:
    IndexLookup(std::vector<std::unique_ptr<IndexLookupInfo>> infos, bool ignoreErrors,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)},
          infos{std::move(infos)}, ignoreErrors(ignoreErrors) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void setBatchInsertSharedState(std::shared_ptr<BatchInsertSharedState> sharedState);

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final;

private:
    void lookup(transaction::Transaction* transaction, const IndexLookupInfo& info);

private:
    std::vector<std::unique_ptr<IndexLookupInfo>> infos;
    bool ignoreErrors;

    std::unique_ptr<IndexLookupLocalState> localState;
};

} // namespace processor
} // namespace kuzu
