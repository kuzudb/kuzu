#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct CommentOnInfo {
    common::table_id_t tableID;
    std::string tableName, comment;
    DataPos outputPos;
    catalog::Catalog* catalog;
    bool hasExecuted = false;

    CommentOnInfo(common::table_id_t tableID, std::string tableName, std::string comment,
        DataPos outputPos, catalog::Catalog* catalog)
        : tableID(tableID), tableName(std::move(tableName)), comment(std::move(comment)),
          outputPos(outputPos), catalog(catalog) {}

    inline std::unique_ptr<CommentOnInfo> copy() {
        return std::make_unique<CommentOnInfo>(tableID, tableName, comment, outputPos, catalog);
    }
};

class CommentOn : public PhysicalOperator {
public:
    CommentOn(std::unique_ptr<CommentOnInfo> localState, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::COMMENT_ON, id, paramsString},
          commentOnInfo{std::move(localState)} {}

    inline bool isSource() const override { return true; }
    inline bool canParallel() const final { return false; }

    inline void initLocalStateInternal(ResultSet* resultSet,
        ExecutionContext* /*context*/) override {
        outputVector = resultSet->getValueVector(commentOnInfo->outputPos).get();
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CommentOn>(commentOnInfo->copy(), id, paramsString);
    }

private:
    std::unique_ptr<CommentOnInfo> commentOnInfo;
    common::ValueVector* outputVector;
};

} // namespace processor
} // namespace kuzu
