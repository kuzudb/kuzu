#include "function/query_fts_index.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/types/internal_id_util.h"
#include "function/fts_index_utils.h"
#include "function/gds/gds_utils.h"
#include "function/query_fts_bind_data.h"
#include "index/fts_index.h"
#include "processor/execution_context.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"
#include "utils/fts_utils.h"

namespace kuzu {
namespace fts_extension {

using namespace storage;
using namespace function;
using namespace binder;
using namespace common;

struct DocScoreInfo {
    struct ScoreData {
        uint64_t df;
        uint64_t tf;

        ScoreData(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
    };

    std::vector<ScoreData> scoreDataCollection;

    explicit DocScoreInfo(ScoreData scoreData) { scoreDataCollection.push_back(scoreData); }

    void addEdge(ScoreData scoreData) { scoreDataCollection.push_back(scoreData); }
};

struct DocScoreCollection {
    std::unordered_map<offset_t, DocScoreInfo> docScores;

    void addDocScoreData(common::offset_t docOffset, DocScoreInfo::ScoreData scoreData) {
        auto tuple = docScores.find(docOffset);
        if (tuple == docScores.end()) {
            docScores.emplace(docOffset, scoreData);
        } else {
            tuple->second.addEdge(scoreData);
        }
    }

    const DocScoreInfo* getDocScoreInfo(common::offset_t docOffset) const {
        auto docScore = docScores.find(docOffset);
        if (docScore == docScores.end()) {
            return nullptr;
        } else {
            return &docScores.at(docOffset);
        }
    }

    idx_t getNumDocsWithScore() const { return docScores.size(); }
};

class QFTSOutputWriter {
public:
    QFTSOutputWriter(const DocScoreCollection& docScoreCollection, MemoryManager* mm,
        QueryFTSConfig config, const QueryFTSBindData& bindData, uint64_t numUniqueTerms);

    void write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
        int64_t docsID);

    std::unique_ptr<QFTSOutputWriter> copy() {
        return std::make_unique<QFTSOutputWriter>(docScoreCollection, mm, config, bindData,
            numUniqueTerms);
    }

private:
    std::unique_ptr<ValueVector> createVector(const LogicalType& type);

private:
    const DocScoreCollection& docScoreCollection;
    MemoryManager* mm;
    QueryFTSConfig config;
    const QueryFTSBindData& bindData;

    std::unique_ptr<ValueVector> docsVector;
    std::unique_ptr<ValueVector> scoreVector;
    std::vector<ValueVector*> vectors;
    uint64_t numUniqueTerms;
};

QFTSOutputWriter::QFTSOutputWriter(const DocScoreCollection& docScoreCollection, MemoryManager* mm,
    QueryFTSConfig config, const QueryFTSBindData& bindData, uint64_t numUniqueTerms)
    : docScoreCollection{docScoreCollection}, mm{mm}, config{config}, bindData{bindData},
      numUniqueTerms{numUniqueTerms} {
    docsVector = createVector(LogicalType::INTERNAL_ID());
    scoreVector = createVector(LogicalType::UINT64());
}

std::unique_ptr<ValueVector> QFTSOutputWriter::createVector(const LogicalType& type) {
    auto vector = std::make_unique<ValueVector>(type.copy(), mm);
    vector->state = DataChunkState::getSingleValueDataChunkState();
    vectors.push_back(vector.get());
    return vector;
}

void QFTSOutputWriter::write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
    int64_t docsID) {
    auto k = config.k;
    auto b = config.b;

    auto docScoreInfo = docScoreCollection.getDocScoreInfo(docsID);
    if (docScoreInfo == nullptr) {
        return;
    }
    double score = 0;
    // If the query is conjunctive, the numbers of distinct terms in the doc and the number of
    // distinct terms in the query must be equal to each other.
    if (config.isConjunctive && docScoreInfo->scoreDataCollection.size() != numUniqueTerms) {
        return;
    }
    auto auxInfo = bindData.entry.getAuxInfo().cast<FTSIndexAuxInfo>();
    for (auto& scoreData : docScoreInfo->scoreDataCollection) {
        auto numDocs = bindData.numDocs;
        auto avgDocLen = bindData.avgDocLen;
        auto df = scoreData.df;
        auto tf = scoreData.tf;
        score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                 ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
    }
    docsVector->setValue(0, nodeID_t{static_cast<offset_t>(docsID), bindData.outputTableID});
    scoreVector->setValue(0, score);
    scoreFT.append(vectors);
}

class QFTSVertexCompute final : public VertexCompute {
public:
    QFTSVertexCompute(MemoryManager* mm, GDSFuncSharedState* sharedState,
        std::unique_ptr<QFTSOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        localFT = sharedState->factorizedTablePool.claimLocalTable(mm);
    }

    ~QFTSVertexCompute() override { sharedState->factorizedTablePool.returnLocalTable(localFT); }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override {
        auto docLens = chunk.getProperties<uint64_t>(0);
        auto docIDs = chunk.getProperties<int64_t>(1);
        for (auto i = 0u; i < chunk.getNodeIDs().size(); i++) {
            writer->write(*localFT, chunk.getNodeIDs()[i], docLens[i], docIDs[i]);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<QFTSVertexCompute>(mm, sharedState, writer->copy());
    }

private:
    MemoryManager* mm;
    GDSFuncSharedState* sharedState;
    processor::FactorizedTable* localFT;
    std::unique_ptr<QFTSOutputWriter> writer;
};

static constexpr char SCORE_PROP_NAME[] = "score";
static constexpr char DOC_FREQUENCY_PROP_NAME[] = "df";
static constexpr char DOC_INFO_PROP_NAME[] = "docInfo";
static constexpr char DOC_LEN_PROP_NAME[] = "len";
static constexpr char DOC_ID_PROP_NAME[] = "docID";

static DocScoreCollection getDFs(main::ClientContext& context,
    const catalog::NodeTableCatalogEntry& termsEntry, const std::vector<std::string>& terms) {
    auto storageManager = context.getStorageManager();
    auto tableID = termsEntry.getTableID();
    auto& termsNodeTable = storageManager->getTable(tableID)->cast<NodeTable>();
    auto tx = context.getTransaction();
    auto docInfoColumnID = termsEntry.getColumnID(DOC_INFO_PROP_NAME);
    auto dfColumnID = termsEntry.getColumnID(DOC_FREQUENCY_PROP_NAME);
    std::vector<LogicalType> vectorTypes;
    vectorTypes.push_back(LogicalType::INTERNAL_ID());
    std::vector<StructField> structFields;
    structFields.emplace_back("docInfo", LogicalType::INT64());
    structFields.emplace_back("tf", LogicalType::UINT64());
    vectorTypes.push_back(LogicalType::LIST(LogicalType::STRUCT(std::move(structFields))));
    vectorTypes.push_back(LogicalType::UINT64());
    auto dataChunk = Table::constructDataChunk(context.getMemoryManager(), std::move(vectorTypes));
    dataChunk.state->getSelVectorUnsafe().setSelSize(1);
    auto nodeIDVector = &dataChunk.getValueVectorMutable(0);
    auto docInfoVector = &dataChunk.getValueVectorMutable(1);
    auto dfVector = &dataChunk.getValueVectorMutable(2);
    auto termsVector = ValueVector(LogicalType::STRING(), context.getMemoryManager());
    termsVector.state = dataChunk.state;
    auto nodeTableScanState =
        NodeTableScanState(nodeIDVector, std::vector{docInfoVector, dfVector}, dataChunk.state);
    nodeTableScanState.setToTable(context.getTransaction(), &termsNodeTable,
        {docInfoColumnID, dfColumnID}, {});
    DocScoreCollection docScoreCollection;
    for (auto& term : terms) {
        termsVector.setValue(0, term);
        offset_t offset = 0;
        if (!termsNodeTable.lookupPK(tx, &termsVector, 0 /* vectorPos */, offset)) {
            continue;
        }
        auto nodeID = nodeID_t{offset, tableID};
        nodeIDVector->setValue(0, nodeID);
        termsNodeTable.initScanState(tx, nodeTableScanState, tableID, offset);
        [[maybe_unused]] auto res = termsNodeTable.lookup(tx, nodeTableScanState);
        auto listEntry = docInfoVector->getValue<list_entry_t>(dataChunk.state->getSelVector()[0]);
        auto dataVector = ListVector::getDataVector(docInfoVector);
        auto docIDVector = StructVector::getFieldVector(dataVector, 0);
        auto tfVector = StructVector::getFieldVector(dataVector, 1);
        auto df = dfVector->getValue<uint64_t>(dataChunk.state->getSelVector()[0]);
        for (auto i = 0u; i < listEntry.size; i++) {
            auto docID = docIDVector->getValue<int64_t>(listEntry.offset + i);
            auto tf = tfVector->getValue<uint64_t>(listEntry.offset + i);
            docScoreCollection.addDocScoreData(docID, {df, tf});
        }
    }
    return docScoreCollection;
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

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto transaction = clientContext->getTransaction();
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    auto graphEntry = graph->getGraphEntry();
    auto qFTSBindData = input.bindData->constPtrCast<QueryFTSBindData>();
    auto& termsEntry = graphEntry->nodeInfos[0].entry->constCast<catalog::NodeTableCatalogEntry>();
    auto terms = qFTSBindData->getTerms(*input.context->clientContext);
    auto docScoreCollection = getDFs(*input.context->clientContext, termsEntry, terms);
    // Do vertex compute to calculate the score for doc with the length property.
    auto mm = clientContext->getMemoryManager();
    auto numUniqueTerms = getNumUniqueTerms(terms);
    auto writer = std::make_unique<QFTSOutputWriter>(docScoreCollection, mm,
        qFTSBindData->getConfig(), *qFTSBindData, numUniqueTerms);
    auto vc = std::make_unique<QFTSVertexCompute>(mm, sharedState, std::move(writer));
    auto vertexPropertiesToScan = std::vector<std::string>{DOC_LEN_PROP_NAME, DOC_ID_PROP_NAME};
    auto docsEntry = graphEntry->nodeInfos[1].entry;
    auto numDocs = clientContext->getStorageManager()
                       ->getTable(docsEntry->getTableID())
                       ->getNumTotalRows(transaction);

    auto docTable =
        clientContext->getStorageManager()->getTable(docsEntry->getTableID())->ptrCast<NodeTable>();
    ValueVector docVector{LogicalType::INT64()};
    docVector.state = DataChunkState::getSingleValueDataChunkState();
    std::vector<common::offset_t> docOffsets;
    for (auto& [docID, _] : docScoreCollection.docScores) {
        common::offset_t offset = INVALID_OFFSET;
        docVector.setValue(0, docID);
        docTable->lookupPK(clientContext->getTransaction(), &docVector, 0 /* vectorPos */, offset);
        docOffsets.push_back(offset);
    }

    if (docScoreCollection.getNumDocsWithScore() < getSparseFrontierSize(numDocs)) {
        auto vertexScanState = graph->prepareVertexScan(docsEntry, vertexPropertiesToScan);
        for (auto& offset : docOffsets) {
            for (auto chunk : graph->scanVertices(offset, offset + 1, *vertexScanState)) {
                vc->vertexCompute(chunk);
            }
        }
    } else {
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vc, docsEntry,
            vertexPropertiesToScan);
    }
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static std::string getParamVal(const TableFuncBindInput& input, idx_t idx) {
    if (input.getParam(idx)->expressionType != ExpressionType::LITERAL) {
        throw BinderException{"The table and index name must be literal expressions."};
    }
    return ExpressionUtil::getLiteralValue<std::string>(
        input.getParam(idx)->constCast<LiteralExpression>());
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    context->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    // For queryFTS, the table and index name must be given at compile time while the user
    // can give the query at runtime.
    auto inputTableName = getParamVal(*input, 0);
    auto indexName = getParamVal(*input, 1);
    auto query = input->getParam(2);

    auto tableEntry = FTSIndexUtils::bindNodeTable(*context, inputTableName, indexName,
        FTSIndexUtils::IndexOperation::QUERY);
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
    auto ftsIndexEntry = catalog->getIndex(transaction, tableEntry->getTableID(), indexName);
    auto entry = catalog->getTableCatalogEntry(transaction, inputTableName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, {entry});

    auto termsEntry = catalog->getTableCatalogEntry(transaction,
        FTSUtils::getTermsTableName(tableEntry->getTableID(), indexName));
    auto docsEntry = catalog->getTableCatalogEntry(transaction,
        FTSUtils::getDocsTableName(tableEntry->getTableID(), indexName));
    auto graphEntry = graph::NativeGraphEntry({termsEntry, docsEntry}, {});

    expression_vector columns;
    auto& docsNode = nodeOutput->constCast<NodeExpression>();
    columns.push_back(docsNode.getInternalID());
    std::string scoreColumnName = SCORE_PROP_NAME;
    if (!input->yieldVariables.empty()) {
        scoreColumnName = GDSFunction::bindColumnName(input->yieldVariables[1], scoreColumnName);
    }
    auto scoreColumn = input->binder->createVariable(scoreColumnName, LogicalType::DOUBLE());
    columns.push_back(scoreColumn);
    auto nodeTable =
        context->getStorageManager()->getTable(ftsIndexEntry->getTableID())->ptrCast<NodeTable>();
    auto index = nodeTable->getIndex(indexName);
    KU_ASSERT(index.has_value());
    auto& ftsIndex = index.value()->cast<FTSIndex>();
    auto& ftsStorageInfo = ftsIndex.getStorageInfo().constCast<FTSStorageInfo>();
    auto bindData =
        std::make_unique<QueryFTSBindData>(std::move(columns), std::move(graphEntry), nodeOutput,
            std::move(query), *ftsIndexEntry, QueryFTSOptionalParams{input->optionalParamsLegacy},
            ftsStorageInfo.numDocs, ftsStorageInfo.avgDocLen);
    context->setUseInternalCatalogEntry(false /* useInternalCatalogEntry */);
    return bindData;
}

function_set QueryFTSFunction::getFunctionSet() {
    function_set result;
    // inputs are tableName, indexName, query
    auto func = std::make_unique<TableFunction>(QueryFTSFunction::name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = GDSFunction::getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
