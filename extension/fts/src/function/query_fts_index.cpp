#include "function/query_fts_index.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/task_system/task_scheduler.h"
#include "common/types/internal_id_util.h"
#include "function/fts_utils.h"
#include "function/gds/gds.h"
#include "function/gds/gds_frontier.h"
#include "function/gds/gds_task.h"
#include "function/gds/gds_utils.h"
#include "function/stem.h"
#include "graph/on_disk_graph.h"
#include "libstemmer.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "re2.h"
#include "storage/index/index_utils.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace fts_extension {

using namespace function;

struct QueryFTSBindData final : GDSBindData {
    std::shared_ptr<Expression> query;
    const FTSIndexCatalogEntry& entry;
    QueryFTSConfig config;
    table_id_t outputTableID;

    QueryFTSBindData(graph::GraphEntry graphEntry, std::shared_ptr<Expression> docs,
        std::shared_ptr<Expression> query, const FTSIndexCatalogEntry& entry, QueryFTSConfig config)
        : GDSBindData{std::move(graphEntry), std::move(docs)}, query{std::move(query)},
          entry{entry}, config{config},
          outputTableID{nodeOutput->constCast<NodeExpression>().getSingleEntry()->getTableID()} {}
    QueryFTSBindData(const QueryFTSBindData& other)
        : GDSBindData{other}, query{other.query}, entry{other.entry}, config{other.config},
          outputTableID{other.outputTableID} {}

    bool hasNodeInput() const override { return false; }

    std::vector<std::string> getTerms() const;

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<QueryFTSBindData>(*this);
    }
};

std::vector<std::string> QueryFTSBindData::getTerms() const {
    if (!binder::ExpressionUtil::canEvaluateAsLiteral(*query)) {
        std::string errMsg;
        switch (query->expressionType) {
        case common::ExpressionType::PARAMETER: {
            errMsg = "The query is a parameter expression. Please assign it a value.";
        } break;
        default: {
            errMsg = "The query must be a parameter/literal expression.";
        } break;
        }
        throw RuntimeException{errMsg};
    }
    auto value = binder::ExpressionUtil::evaluateAsLiteralValue(*query);
    if (value.getDataType() != common::LogicalType::STRING()) {
        throw RuntimeException{"The query must be a string literal."};
    }
    auto queryInStr = value.getValue<std::string>();
    auto stemmer = entry.getFTSConfig().stemmer;
    std::string regexPattern = "[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\/\\|'\"`-]+";
    std::string replacePattern = " ";
    RE2::GlobalReplace(&queryInStr, regexPattern, replacePattern);
    StringUtils::toLower(queryInStr);
    auto terms = StringUtils::split(queryInStr, " ");
    if (stemmer == "none") {
        return terms;
    }
    StemFunction::validateStemmer(stemmer);
    auto sbStemmer = sb_stemmer_new(reinterpret_cast<const char*>(stemmer.c_str()), "UTF_8");
    std::vector<std::string> result;
    for (auto& term : terms) {
        auto stemData = sb_stemmer_stem(sbStemmer, reinterpret_cast<const sb_symbol*>(term.c_str()),
            term.length());
        result.push_back(reinterpret_cast<const char*>(stemData));
    }
    sb_stemmer_delete(sbStemmer);
    return result;
}

struct ScoreData {
    uint64_t df;
    uint64_t tf;

    ScoreData(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
};

struct ScoreInfo {
    std::vector<ScoreData> scoreData;

    void addEdge(uint64_t df, uint64_t tf) { scoreData.emplace_back(df, tf); }
};

class QFTSOutputWriter {
public:
    QFTSOutputWriter(storage::MemoryManager* mm, const QueryFTSBindData& bindData,
        uint64_t numUniqueTerms)
        : mm{mm}, bindData{bindData}, numUniqueTerms{numUniqueTerms} {
        docsVector = createVector(LogicalType::INTERNAL_ID());
        scoreVector = createVector(LogicalType::UINT64());
    }

    void write(processor::FactorizedTable& table, const ScoreInfo& scoreInfo,
        uint64_t len, int64_t docsID) {
        auto k = bindData.config.k;
        auto b = bindData.config.b;

        double score = 0;
        // If the query is conjunctive, the numbers of distinct terms in the doc and the number of
        // distinct terms in the query must be equal to each other.
        if (bindData.config.isConjunctive && scoreInfo.scoreData.size() != numUniqueTerms) {
            return;
        }
        for (auto& scoreData : scoreInfo.scoreData) {
            auto numDocs = bindData.entry.getNumDocs();
            auto avgDocLen = bindData.entry.getAvgDocLen();
            auto df = scoreData.df;
            auto tf = scoreData.tf;
            score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                     ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
        }
        docsVector->setValue(0, nodeID_t{static_cast<offset_t>(docsID), bindData.outputTableID});
        scoreVector->setValue(0, score);
        table.append(vectors);
    }

    std::unique_ptr<QFTSOutputWriter> copy() {
        return std::make_unique<QFTSOutputWriter>(mm, bindData, numUniqueTerms);
    }

private:
    std::unique_ptr<common::ValueVector> createVector(const LogicalType& type) {
        auto vector = std::make_unique<ValueVector>(type.copy(), mm);
        vector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(vector.get());
        return vector;
    }

private:
    storage::MemoryManager* mm;
    const QueryFTSBindData& bindData;

    std::unique_ptr<ValueVector> docsVector;
    std::unique_ptr<ValueVector> scoreVector;
    std::vector<ValueVector*> vectors;
    uint64_t numUniqueTerms;
};

class QFTSVertexCompute final : public VertexCompute {
public:
    QFTSVertexCompute(storage::MemoryManager* mm, processor::GDSCallSharedState* sharedState,
        std::unique_ptr<QFTSOutputWriter> writer, const node_id_map_t<ScoreInfo>& scores)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)}, scores{scores} {
        localFT = sharedState->claimLocalTable(mm);
    }

    ~QFTSVertexCompute() override { sharedState->returnLocalTable(localFT); }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override {
        auto docLens = chunk.getProperties<uint64_t>(0);
        auto docIDs = chunk.getProperties<int64_t>(1);
        auto nodeIDs = chunk.getNodeIDs();
        for (auto i = 0u; i < chunk.getNodeIDs().size(); i++) {
            if (!scores.contains(nodeIDs[i])) {
                continue;
            }
            writer->write(*localFT, scores.at(nodeIDs[i]), docLens[i], docIDs[i]);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<QFTSVertexCompute>(mm, sharedState, writer->copy(), scores);
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    processor::FactorizedTable* localFT;
    std::unique_ptr<QFTSOutputWriter> writer;

    const node_id_map_t<ScoreInfo>& scores;
};

static std::unordered_map<common::offset_t, uint64_t> getDFs(main::ClientContext& context,
    const catalog::NodeTableCatalogEntry& termsEntry, const std::vector<std::string>& terms) {
    auto storageManager = context.getStorageManager();
    auto tableID = termsEntry.getTableID();
    auto& termsNodeTable = storageManager->getTable(tableID)->cast<storage::NodeTable>();
    auto tx = context.getTransaction();
    auto dfColumnID = termsEntry.getColumnID(QueryFTSAlgorithm::DOC_FREQUENCY_PROP_NAME);
    auto dfColumn = &termsNodeTable.getColumn(dfColumnID);
    std::vector<common::LogicalType> vectorTypes;
    vectorTypes.push_back(LogicalType::UINT64());
    vectorTypes.push_back(LogicalType::INTERNAL_ID());
    auto dataChunk =
        storage::Table::constructDataChunk(context.getMemoryManager(), std::move(vectorTypes));
    dataChunk.state->getSelVectorUnsafe().setSelSize(1);
    auto dfVector = &dataChunk.getValueVector(0);
    auto nodeIDVector = &dataChunk.getValueVectorMutable(1);
    auto termsVector = ValueVector(LogicalType::STRING(), context.getMemoryManager());
    termsVector.state = dataChunk.state;
    auto nodeTableScanState =
        storage::NodeTableScanState{tableID, {dfColumnID}, {dfColumn}, dataChunk, nodeIDVector};
    std::unordered_map<common::offset_t, uint64_t> dfs;
    for (auto& term : terms) {
        termsVector.setValue(0, term);
        common::offset_t offset = 0;
        if (!termsNodeTable.lookupPK(tx, &termsVector, 0, offset)) {
            continue;
        }
        auto nodeID = nodeID_t{offset, tableID};
        nodeIDVector->setValue(0, nodeID);
        termsNodeTable.initScanState(tx, nodeTableScanState, tableID, offset);
        termsNodeTable.lookup(tx, nodeTableScanState);
        dfs.emplace(offset, dfVector->getValue<uint64_t>(0));
    }
    return dfs;
}

static uint64_t getNumUniqueTerms(const std::vector<std::string>& terms) {
    auto uniqueTerms = std::unordered_set<std::string>{terms.begin(), terms.end()};
    return uniqueTerms.size();
}

// A size is considered as sparse if it is smaller than 0.1% of table size of less than 1000.
static constexpr double SPARSE_PERCENTAGE = 0.001;
static constexpr uint64_t MIN_SPARSE_SIZE = 1000;
static uint64_t getSparseFrontierSize(uint64_t numRows) {
    uint64_t size = numRows * SPARSE_PERCENTAGE;
    if (size < MIN_SPARSE_SIZE) {
        return MIN_SPARSE_SIZE;
    }
    return size;
}

void QueryFTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto clientContext = executionContext->clientContext;
    auto storageManager = clientContext->getStorageManager();
    auto transaction = clientContext->getTransaction();
    auto graph = sharedState->graph.get();
    auto graphEntry = graph->getGraphEntry();
    auto qFTSBindData = bindData->ptrCast<QueryFTSBindData>();
    auto& termsEntry = graphEntry->nodeEntries[0]->constCast<catalog::NodeTableCatalogEntry>();
    auto termsTableID = termsEntry.getTableID();
    auto terms = bindData->ptrCast<QueryFTSBindData>()->getTerms();
    auto dfs = getDFs(*clientContext, termsEntry, terms);
    // The assumption is that dfs is small. So we don't execute with dense edge compute.
    // Also, the ScoreInfo needs to be a thread safe data structure in order to be parallelized.
    node_id_map_t<ScoreInfo> scores;
    auto edgePropertyToScan = QueryFTSAlgorithm::TERM_FREQUENCY_PROP_NAME;
    auto edgeScanState = graph->prepareRelScan(graphEntry->relEntries[0], edgePropertyToScan);
    for (auto& [offset, df] : dfs) {
        auto nodeID = nodeID_t{offset, termsTableID};
        for (auto chunk : graph->scanFwd(nodeID, *edgeScanState)) {
            chunk.forEach<uint64_t>([&scores, &df](auto docNodeID, auto /* edgeID */, auto tf) {
                if (!scores.contains(docNodeID)) {
                    scores.emplace(docNodeID, ScoreInfo{});
                }
                scores.at(docNodeID).addEdge(df, tf);
            });
        }
    }
    // Do vertex compute to calculate the score for doc with the length property.
    auto mm = clientContext->getMemoryManager();
    auto numUniqueTerms = getNumUniqueTerms(terms);
    auto writer = std::make_unique<QFTSOutputWriter>(mm, *qFTSBindData, numUniqueTerms);
    auto vertexPropertiesToScan = std::vector<std::string>{QueryFTSAlgorithm::DOC_LEN_PROP_NAME,
        QueryFTSAlgorithm::DOC_ID_PROP_NAME};
    auto docsEntry = graphEntry->nodeEntries[1];
    auto numDocs = storageManager->getTable(docsEntry->getTableID())->getNumTotalRows(transaction);
    if (scores.size() < getSparseFrontierSize(numDocs)) {
        auto vertexScanState = graph->prepareVertexScan(docsEntry, vertexPropertiesToScan);
        for (auto& [nodeID, scoreInfo] : scores) {
            for (auto chunk :
                graph->scanVertices(nodeID.offset, nodeID.offset + 1, *vertexScanState)) {
                auto docLens = chunk.getProperties<uint64_t>(0);
                auto docIDs = chunk.getProperties<int64_t>(1);
                for (auto i = 0u; i < chunk.getNodeIDs().size(); i++) {
                    writer->write(*sharedState->fTable, scoreInfo, docLens[i], docIDs[i]);
                }
            }
        }
    } else {
        auto vc = std::make_unique<QFTSVertexCompute>(mm, sharedState.get(), std::move(writer), scores);
        GDSUtils::runVertexCompute(executionContext, graph, *vc, docsEntry, vertexPropertiesToScan);
    }
    sharedState->mergeLocalTables();
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(QueryFTSAlgorithm::SCORE_PROP_NAME, LogicalType::DOUBLE());
}

expression_vector QueryFTSAlgorithm::getResultColumns(Binder* binder) const {
    expression_vector columns;
    auto& docsNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(docsNode.getInternalID());
    columns.push_back(getScoreColumn(binder));
    return columns;
}

static std::string getParamVal(const GDSBindInput& input, idx_t idx) {
    if (input.getParam(idx)->expressionType != common::ExpressionType::LITERAL) {
        throw common::BinderException{"The table and index name must be literal expressions."};
    }
    return ExpressionUtil::getLiteralValue<std::string>(
        input.getParam(idx)->constCast<LiteralExpression>());
}

void QueryFTSAlgorithm::bind(const GDSBindInput& input, main::ClientContext& context) {
    context.setToUseInternalCatalogEntry();
    // For queryFTS, the table and index name must be given at compile time while the user
    // can give the query at runtime.
    auto inputTableName = getParamVal(input, 0);
    auto indexName = getParamVal(input, 1);
    auto query = input.getParam(2);

    auto tableEntry = storage::IndexUtils::bindTable(context, inputTableName, indexName,
        storage::IndexOperation::QUERY);
    auto& ftsIndexEntry =
        context.getCatalog()
            ->getIndex(context.getTransaction(), tableEntry->getTableID(), indexName)
            ->constCast<FTSIndexCatalogEntry>();
    auto entry =
        context.getCatalog()->getTableCatalogEntry(context.getTransaction(), inputTableName);
    auto nodeOutput = bindNodeOutput(input.binder, {entry});

    auto termsEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getTermsTableName(tableEntry->getTableID(), indexName));
    auto docsEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getDocsTableName(tableEntry->getTableID(), indexName));
    auto appearsInEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getAppearsInTableName(tableEntry->getTableID(), indexName));
    auto graphEntry = graph::GraphEntry({termsEntry, docsEntry}, {appearsInEntry});
    bindData = std::make_unique<QueryFTSBindData>(std::move(graphEntry), nodeOutput,
        std::move(query), ftsIndexEntry, QueryFTSConfig{input.optionalParams});
}

function_set QueryFTSFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<QueryFTSAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
