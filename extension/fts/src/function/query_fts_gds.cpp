#include "function/query_fts_gds.h"

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/runtime.h"
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

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace fts_extension {

using namespace function;

struct QFTSGDSBindData final : public function::GDSBindData {
    std::vector<std::string> terms;
    uint64_t numDocs;
    double_t avgDocLen;
    QueryFTSConfig config;
    common::table_id_t outputTableID;

    QFTSGDSBindData(std::vector<std::string> terms, graph::GraphEntry graphEntry,
        std::shared_ptr<binder::Expression> docs, uint64_t numDocs, double_t avgDocLen,
        QueryFTSConfig config)
        : GDSBindData{std::move(graphEntry), std::move(docs)}, terms{std::move(terms)},
          numDocs{numDocs}, avgDocLen{avgDocLen}, config{config},
          outputTableID{nodeOutput->constCast<NodeExpression>().getSingleEntry()->getTableID()} {}
    QFTSGDSBindData(const QFTSGDSBindData& other)
        : GDSBindData{other}, terms{other.terms}, numDocs{other.numDocs},
          avgDocLen{other.avgDocLen}, config{other.config}, outputTableID{other.outputTableID} {}

    bool hasNodeInput() const override { return false; }

    uint64_t getNumUniqueTerms() const;

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<QFTSGDSBindData>(*this);
    }
};

uint64_t QFTSGDSBindData::getNumUniqueTerms() const {
    auto uniqueTerms = std::unordered_set<std::string>{terms.begin(), terms.end()};
    return uniqueTerms.size();
}

struct ScoreData {
    uint64_t df;
    uint64_t tf;

    ScoreData(uint64_t df, uint64_t tf) : df{df}, tf{tf} {}
};

struct ScoreInfo {
    nodeID_t termID;
    std::vector<ScoreData> scoreData;

    explicit ScoreInfo(nodeID_t termID) : termID{termID} {}

    void addEdge(uint64_t df, uint64_t tf) { scoreData.emplace_back(df, tf); }
};

struct QFTSEdgeCompute : public EdgeCompute {
    DoublePathLengthsFrontierPair* termsFrontier;
    common::node_id_map_t<ScoreInfo>* scores;
    common::node_id_map_t<uint64_t>* dfs;

    QFTSEdgeCompute(DoublePathLengthsFrontierPair* termsFrontier,
        common::node_id_map_t<ScoreInfo>* scores, common::node_id_map_t<uint64_t>* dfs)
        : termsFrontier{termsFrontier}, scores{scores}, dfs{dfs} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& resultChunk,
        bool) override;

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<QFTSEdgeCompute>(termsFrontier, scores, dfs);
    }
};

std::vector<nodeID_t> QFTSEdgeCompute::edgeCompute(nodeID_t boundNodeID,
    graph::NbrScanState::Chunk& resultChunk, bool) {
    KU_ASSERT(dfs->contains(boundNodeID));
    std::vector<nodeID_t> activeNodes;
    resultChunk.forEach<uint64_t>([&](auto docNodeID, auto /* edgeID */, auto tf) {
        auto df = dfs->at(boundNodeID);
        if (!scores->contains(docNodeID)) {
            scores->emplace(docNodeID, ScoreInfo{boundNodeID});
        }
        scores->at(docNodeID).addEdge(df, tf);
        activeNodes.push_back(docNodeID);
    });
    return activeNodes;
}

struct QFTSOutput {
    common::node_id_map_t<ScoreInfo> scores;

    QFTSOutput() = default;
    virtual ~QFTSOutput() = default;
};

struct QFTSState : public function::GDSComputeState {
    void initFirstFrontierWithTerms(const common::node_id_map_t<uint64_t>& dfs,
        common::table_id_t termsTableID) const;

    QFTSState(std::unique_ptr<function::FrontierPair> frontierPair,
        std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termsTableID);
};

QFTSState::QFTSState(std::unique_ptr<function::FrontierPair> frontierPair,
    std::unique_ptr<function::EdgeCompute> edgeCompute, common::table_id_t termsTableID)
    : function::GDSComputeState{std::move(frontierPair), std::move(edgeCompute),
          nullptr /* outputNodeMask */} {
    this->frontierPair->pinNextFrontier(termsTableID);
}

void QFTSState::initFirstFrontierWithTerms(const common::node_id_map_t<uint64_t>& dfs,
    common::table_id_t termsTableID) const {
    SparseFrontier sparseFrontier;
    sparseFrontier.pinTableID(termsTableID);
    for (auto& [nodeID, df] : dfs) {
        frontierPair->addNodeToNextDenseFrontier(nodeID);
        sparseFrontier.addNode(nodeID);
        sparseFrontier.checkSampleSize();
    }
    frontierPair->mergeLocalFrontier(sparseFrontier);
}

void runFrontiersOnce(processor::ExecutionContext* executionContext, QFTSState& qFtsState,
    graph::Graph* graph, common::idx_t tfPropertyIdx) {
    auto frontierPair = qFtsState.frontierPair.get();
    frontierPair->beginNewIteration();
    auto relTableIDInfos = graph->getRelTableIDInfos();
    auto& appearsInTableInfo = relTableIDInfos[0];
    frontierPair->beginFrontierComputeBetweenTables(appearsInTableInfo.fromNodeTableID,
        appearsInTableInfo.toNodeTableID);
    GDSUtils::scheduleFrontierTask(appearsInTableInfo.toNodeTableID, appearsInTableInfo.relTableID,
        graph, ExtendDirection::FWD, qFtsState, executionContext, 1 /* numThreads */,
        tfPropertyIdx);
}

class QFTSOutputWriter {
public:
    QFTSOutputWriter(storage::MemoryManager* mm, QFTSOutput* qFTSOutput,
        const QFTSGDSBindData& bindData);

    void write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
        int64_t docsID);

    std::unique_ptr<QFTSOutputWriter> copy();

private:
    QFTSOutput* qFTSOutput;
    common::ValueVector docsVector;
    common::ValueVector scoreVector;
    std::vector<common::ValueVector*> vectors;
    common::idx_t pos;
    storage::MemoryManager* mm;
    const QFTSGDSBindData& bindData;
    uint64_t numUniqueTerms;
};

QFTSOutputWriter::QFTSOutputWriter(storage::MemoryManager* mm, QFTSOutput* qFTSOutput,
    const QFTSGDSBindData& bindData)
    : qFTSOutput{qFTSOutput}, docsVector{LogicalType::INTERNAL_ID(), mm},
      scoreVector{LogicalType::UINT64(), mm}, mm{mm}, bindData{bindData} {
    auto state = DataChunkState::getSingleValueDataChunkState();
    pos = state->getSelVector()[0];
    docsVector.setState(state);
    scoreVector.setState(state);
    vectors.push_back(&docsVector);
    vectors.push_back(&scoreVector);
    numUniqueTerms = bindData.getNumUniqueTerms();
}

void QFTSOutputWriter::write(processor::FactorizedTable& scoreFT, nodeID_t docNodeID, uint64_t len,
    int64_t docsID) {
    bool hasScore = qFTSOutput->scores.contains(docNodeID);
    docsVector.setNull(pos, !hasScore);
    scoreVector.setNull(pos, !hasScore);
    auto k = bindData.config.k;
    auto b = bindData.config.b;
    if (hasScore) {
        auto scoreInfo = qFTSOutput->scores.at(docNodeID);
        double score = 0;
        // If the query is conjunctive, the numbers of distinct terms in the doc and the number of
        // distinct terms in the query must be equal to each other.
        if (bindData.config.isConjunctive &&
            scoreInfo.scoreData.size() != bindData.getNumUniqueTerms()) {
            return;
        }
        for (auto& scoreData : scoreInfo.scoreData) {
            auto numDocs = bindData.numDocs;
            auto avgDocLen = bindData.avgDocLen;
            auto df = scoreData.df;
            auto tf = scoreData.tf;
            score += log10((numDocs - df + 0.5) / (df + 0.5) + 1) *
                     ((tf * (k + 1) / (tf + k * (1 - b + b * (len / avgDocLen)))));
        }
        docsVector.setValue(pos, nodeID_t{(common::offset_t)docsID, bindData.outputTableID});
        scoreVector.setValue(pos, score);
    }
    scoreFT.append(vectors);
}

std::unique_ptr<QFTSOutputWriter> QFTSOutputWriter::copy() {
    return std::make_unique<QFTSOutputWriter>(mm, qFTSOutput, bindData);
}

class QFTSVertexCompute : public VertexCompute {
public:
    explicit QFTSVertexCompute(storage::MemoryManager* mm,
        processor::GDSCallSharedState* sharedState, std::unique_ptr<QFTSOutputWriter> writer)
        : mm{mm}, sharedState{sharedState}, writer{std::move(writer)} {
        scoreFT = sharedState->claimLocalTable(mm);
    }

    ~QFTSVertexCompute() override { sharedState->returnLocalTable(scoreFT); }

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<QFTSVertexCompute>(mm, sharedState, writer->copy());
    }

private:
    storage::MemoryManager* mm;
    processor::GDSCallSharedState* sharedState;
    processor::FactorizedTable* scoreFT;
    std::unique_ptr<QFTSOutputWriter> writer;
};

struct TermsComputeSharedState {
    std::vector<std::string> queryTerms;
    common::node_id_map_t<uint64_t> dfs;
    std::mutex mtx;

    explicit TermsComputeSharedState(std::vector<std::string> queryTerms)
        : queryTerms{std::move(queryTerms)} {}
};

class TermsVertexCompute : public VertexCompute {
public:
    explicit TermsVertexCompute(TermsComputeSharedState* sharedState) : sharedState{sharedState} {}

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override;

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<TermsVertexCompute>(sharedState);
    }

private:
    TermsComputeSharedState* sharedState;
};

void TermsVertexCompute::vertexCompute(const graph::VertexScanState::Chunk& chunk) {
    auto nodeIDs = chunk.getNodeIDs();
    auto terms = chunk.getProperties<common::ku_string_t>(0);
    auto dfs = chunk.getProperties<uint64_t>(1);
    for (auto i = 0u; i < nodeIDs.size(); i++) {
        if (std::find(sharedState->queryTerms.begin(), sharedState->queryTerms.end(),
                terms[i].getAsString()) != sharedState->queryTerms.end()) {
            std::lock_guard<std::mutex> lock(sharedState->mtx);
            sharedState->dfs.emplace(nodeIDs[i], dfs[i]);
        }
    }
}

void QFTSVertexCompute::vertexCompute(const graph::VertexScanState::Chunk& chunk) {
    auto docLens = chunk.getProperties<uint64_t>(0);
    auto docIDs = chunk.getProperties<int64_t>(1);
    for (auto i = 0u; i < chunk.getNodeIDs().size(); i++) {
        writer->write(*scoreFT, chunk.getNodeIDs()[i], docLens[i], docIDs[i]);
    }
}

void QFTSAlgorithm::exec(processor::ExecutionContext* executionContext) {
    auto termsTableID = sharedState->graph->getNodeTableIDs()[0];
    auto output = std::make_unique<QFTSOutput>();

    // Do vertex compute to get the terms
    auto termsTableEntry = executionContext->clientContext->getCatalog()->getTableCatalogEntry(
        executionContext->clientContext->getTransaction(), termsTableID);
    const graph::GraphEntry entry{{termsTableEntry}, {} /* relTableEntries */};
    graph::OnDiskGraph graph(executionContext->clientContext, entry);
    auto termsComputeSharedState =
        TermsComputeSharedState{bindData->ptrCast<QFTSGDSBindData>()->terms};
    TermsVertexCompute termsCompute{&termsComputeSharedState};
    GDSUtils::runVertexCompute(executionContext, &graph, termsCompute,
        std::vector<std::string>{"term", "df"});

    // Do edge compute to extend terms -> docs and save the term frequency and document frequency
    // for each term-doc pair. The reason why we store the term frequency and document frequency
    // is that: we need the `len` property from the docs table which is only available during the
    // vertex compute.
    auto currentFrontier = getPathLengthsFrontier(executionContext, PathLengths::UNVISITED);
    auto nextFrontier = getPathLengthsFrontier(executionContext, PathLengths::UNVISITED);
    auto frontierPair = std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier,
        nextFrontier, 1 /* numThreads */);
    auto edgeCompute = std::make_unique<QFTSEdgeCompute>(frontierPair.get(), &output->scores,
        &termsComputeSharedState.dfs);

    auto clientContext = executionContext->clientContext;
    auto transaction = clientContext->getTransaction();
    auto catalog = clientContext->getCatalog();
    auto mm = clientContext->getMemoryManager();
    QFTSState qFTSState = QFTSState{std::move(frontierPair), std::move(edgeCompute), termsTableID};
    qFTSState.initFirstFrontierWithTerms(termsComputeSharedState.dfs, termsTableID);

    runFrontiersOnce(executionContext, qFTSState, sharedState->graph.get(),
        catalog->getTableCatalogEntry(transaction, sharedState->graph->getRelTableIDs()[0])
            ->getPropertyIdx(QFTSAlgorithm::TERM_FREQUENCY_PROP_NAME));

    // Do vertex compute to calculate the score for doc with the length property.

    auto writer =
        std::make_unique<QFTSOutputWriter>(mm, output.get(), *bindData->ptrCast<QFTSGDSBindData>());
    auto writerVC = std::make_unique<QFTSVertexCompute>(mm, sharedState.get(), std::move(writer));
    auto docsTableID = sharedState->graph->getNodeTableIDs()[1];
    GDSUtils::runVertexCompute(executionContext, sharedState->graph.get(), *writerVC, docsTableID,
        {QFTSAlgorithm::DOC_LEN_PROP_NAME, QFTSAlgorithm::DOC_ID_PROP_NAME});
    sharedState->mergeLocalTables();
}

static std::shared_ptr<Expression> getScoreColumn(Binder* binder) {
    return binder->createVariable(QFTSAlgorithm::SCORE_PROP_NAME, LogicalType::DOUBLE());
}

binder::expression_vector QFTSAlgorithm::getResultColumns(binder::Binder* binder) const {
    expression_vector columns;
    auto& docsNode = bindData->getNodeOutput()->constCast<NodeExpression>();
    columns.push_back(docsNode.getInternalID());
    columns.push_back(getScoreColumn(binder));
    return columns;
}

static std::string getParamVal(const GDSBindInput& input, idx_t idx) {
    return ExpressionUtil::getLiteralValue<std::string>(
        input.getParam(idx)->constCast<LiteralExpression>());
}

static std::vector<std::string> getTerms(std::string& query, const std::string& stemmer) {
    std::string regexPattern = "[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\/\\|'\"`-]+";
    std::string replacePattern = " ";
    RE2::GlobalReplace(&query, regexPattern, replacePattern);
    StringUtils::toLower(query);
    auto terms = StringUtils::split(query, " ");
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

void QFTSAlgorithm::bind(const GDSBindInput& input, main::ClientContext& context) {
    auto inputTableName = getParamVal(input, 0);
    auto indexName = getParamVal(input, 1);
    auto query = getParamVal(input, 2);
    auto stemmer = "english";
    auto terms = getTerms(query, stemmer);

    auto& tableEntry =
        FTSUtils::bindTable(inputTableName, &context, indexName, FTSUtils::IndexOperation::QUERY);
    FTSUtils::validateIndexExistence(context, tableEntry.getTableID(), indexName);
    auto& ftsIndexEntry =
        context.getCatalog()
            ->getIndex(context.getTransaction(), tableEntry.getTableID(), indexName)
            ->constCast<FTSIndexCatalogEntry>();
    auto entry =
        context.getCatalog()->getTableCatalogEntry(context.getTransaction(), inputTableName);
    auto nodeOutput = bindNodeOutput(input.binder, {entry});

    auto termsEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getTermsTableName(tableEntry.getTableID(), indexName));
    auto docsEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getDocsTableName(tableEntry.getTableID(), indexName));
    auto appearsInEntry = context.getCatalog()->getTableCatalogEntry(context.getTransaction(),
        FTSUtils::getAppearsInTableName(tableEntry.getTableID(), indexName));
    auto graphEntry = graph::GraphEntry({termsEntry, docsEntry}, {appearsInEntry});
    bindData = std::make_unique<QFTSGDSBindData>(std::move(terms), std::move(graphEntry),
        nodeOutput, ftsIndexEntry.getNumDocs(), ftsIndexEntry.getAvgDocLen(),
        QueryFTSConfig{input.optionalParams});
}

function::function_set QFTSFunction::getFunctionSet() {
    function_set result;
    auto algo = std::make_unique<QFTSAlgorithm>();
    result.push_back(
        std::make_unique<GDSFunction>(name, algo->getParameterTypeIDs(), std::move(algo)));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
