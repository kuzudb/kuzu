#include "update/update_fts.h"

#include "catalog/fts_index_catalog_entry.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;

FTSUpdater::FTSUpdater()
    : docNodeIDVector{LogicalType::INTERNAL_ID()}, docPKVector{LogicalType::INT64()},
      lenVector{LogicalType::UINT64()}, dictNodeIDVector{LogicalType::INTERNAL_ID()},
      dictPKVector{LogicalType::STRING()}, dictDFVector{LogicalType::UINT64()} {
    docProperties.push_back(&lenVector);
    dictProperties.push_back(&dictDFVector);
}

void FTSUpdater::insertNode(main::ClientContext* context, common::nodeID_t insertedNodeID,
    std::vector<common::ValueVector*> columnDataVectors) {
    std::string content;
    std::vector<std::string> terms;
    for (auto dataVectorIdx : dataVectorIdxes) {
        auto columnDataVector = columnDataVectors[dataVectorIdx];
        KU_ASSERT(columnDataVector->state->isFlat());
        auto pos = columnDataVector->state->getSelVector()[0];
        if (columnDataVector->isNull(pos)) {
            continue;
        }
        content = columnDataVector->getValue<ku_string_t>(pos).getAsString();
        FTSUtils::normalizeQuery(content);
        terms = StringUtils::split(content, " ");
        auto stemmedTerms =
            FTSUtils::stemTerms(terms, ftsEntry->getAuxInfo().cast<FTSIndexAuxInfo>().config,
                *context, false /* isConjunctive */);
        terms.insert(terms.end(), stemmedTerms.begin(), stemmedTerms.end());
    }

    std::unordered_map<std::string, uint64_t> tfMap;
    for (auto& term : terms) {
        ++tfMap[term];
    }

    docPKVector.setValue(0, insertedNodeID.offset);
    lenVector.setValue(0, terms.size());
    // Insert to doc table
    auto nodeInsertState = std::make_unique<storage::NodeTableInsertState>(docNodeIDVector,
        docPKVector, docProperties);
    docTable->insert(context->getTransaction(), *nodeInsertState);

    // Insert to dict table

    // Insert to appears in table
}

void FTSUpdater::deleteNode(transaction::Transaction* transaction, common::nodeID_t deletedNodeID) {

}

} // namespace fts_extension
} // namespace kuzu
