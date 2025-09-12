#include "function/query_fts/query_fts_pattern_match.h"

#include <set>

#include "catalog/fts_index_catalog_entry.h"
#include "function/gds/compute.h"
#include "function/gds/gds_utils.h"
#include "function/query_fts/query_fts_bind_data.h"
#include "function/query_fts/query_fts_term_lookup.h"
#include "libstemmer.h"
#include "storage/storage_manager.h"
#include "utils/fts_utils.h"

using namespace kuzu::function;
using namespace kuzu::processor;

namespace kuzu {
namespace fts_extension {

class MatchTermVertexCompute : public function::VertexCompute {
public:
    MatchTermVertexCompute(std::vector<VCQueryTerm>& queryTerms,
        std::unordered_map<common::offset_t, uint64_t>& resDfs)
        : queryTerms{queryTerms}, resDfs{resDfs} {}

    virtual void handleMatchedTerm(uint64_t itr, const graph::VertexScanState::Chunk& chunk) = 0;

    void vertexCompute(const graph::VertexScanState::Chunk& chunk) override {
        auto terms = chunk.getProperties<common::ku_string_t>(0);
        for (auto& queryTerm : queryTerms) {
            // queryTerm.index() is 0 for string, 1 for unique_ptr<RE2>
            if (queryTerm.index() == 0) {
                std::string& queryString = std::get<0>(queryTerm);
                for (auto i = 0u; i < chunk.size(); ++i) {
                    if (queryString == terms[i].getAsString()) {
                        handleMatchedTerm(i, chunk);
                    }
                }
            } else {
                RE2& regex = *std::get<1>(queryTerm);
                for (auto i = 0u; i < chunk.size(); ++i) {
                    if (RE2::FullMatch(terms[i].getAsString(), regex)) {
                        handleMatchedTerm(i, chunk);
                    }
                }
            }
        }
    }

protected:
    std::vector<VCQueryTerm>& queryTerms;
    std::unordered_map<common::offset_t, uint64_t>& resDfs;
};

class StemTermMatchVertexCompute final : public MatchTermVertexCompute {
public:
    explicit StemTermMatchVertexCompute(std::unordered_map<common::offset_t, uint64_t>& resDfs,
        std::vector<VCQueryTerm>& queryTerms)
        : MatchTermVertexCompute{queryTerms, resDfs} {}

    void handleMatchedTerm(uint64_t itr, const graph::VertexScanState::Chunk& chunk) override {
        auto dfs = chunk.getProperties<uint64_t>(1);
        auto nodeIds = chunk.getNodeIDs();
        resDfs[nodeIds[itr].offset] = dfs[itr];
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<StemTermMatchVertexCompute>(resDfs, queryTerms);
    }
};

class ExactTermMatchVertexCompute final : public MatchTermVertexCompute {
public:
    ExactTermMatchVertexCompute(std::unordered_map<common::offset_t, uint64_t>& resDfs,
        std::vector<VCQueryTerm>& queryTerms, const QueryFTSBindData& bindData,
        main::ClientContext& context)
        : MatchTermVertexCompute{queryTerms, resDfs},
          sbStemmer{sb_stemmer_new(
              reinterpret_cast<const char*>(
                  bindData.entry.getAuxInfo().cast<FTSIndexAuxInfo>().config.stemmer.c_str()),
              "UTF_8")},
          bindData{bindData}, context{context},
          termsDFLookup{bindData.getTermsEntry(context), context} {}

    ~ExactTermMatchVertexCompute() override { sb_stemmer_delete(sbStemmer); }

    void handleMatchedTerm(uint64_t itr, const graph::VertexScanState::Chunk& chunk) override {
        auto term = chunk.getProperties<common::ku_string_t>(0)[itr];
        auto stemData = sb_stemmer_stem(sbStemmer,
            reinterpret_cast<const sb_symbol*>(term.getData()), term.len);
        auto result = termsDFLookup.lookupTermDF(reinterpret_cast<const char*>(stemData));
        KU_ASSERT(result.first != common::INVALID_OFFSET);
        resDfs.insert(result);
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<ExactTermMatchVertexCompute>(resDfs, queryTerms, bindData, context);
    }

private:
    sb_stemmer* sbStemmer;
    const QueryFTSBindData& bindData;
    main::ClientContext& context;
    TermsDFLookup termsDFLookup;
};

static void stemTermMatch(std::unordered_map<common::offset_t, uint64_t>& dfs,
    std::vector<VCQueryTerm>& vcQueryTerms, ExecutionContext* executionContext, graph::Graph* graph,
    const QueryFTSBindData& bindData) {
    auto matchVc = StemTermMatchVertexCompute{dfs, vcQueryTerms};
    GDSUtils::runVertexCompute(executionContext, GDSDensityState::DENSE, graph, matchVc,
        bindData.getTermsEntry(*executionContext->clientContext),
        std::vector<std::string>{"term", TermsDFLookup::DOC_FREQUENCY_PROP_NAME});
}

static void exactTermMatch(std::unordered_map<common::offset_t, uint64_t>& dfs,
    std::vector<VCQueryTerm>& vcQueryTerms, ExecutionContext* executionContext, graph::Graph* graph,
    const QueryFTSBindData& bindData) {
    auto matchOrigTermVc =
        ExactTermMatchVertexCompute{dfs, vcQueryTerms, bindData, *executionContext->clientContext};
    GDSUtils::runVertexCompute(executionContext, GDSDensityState::DENSE, graph, matchOrigTermVc,
        bindData.getOrigTermsEntry(*executionContext->clientContext),
        std::vector<std::string>{"term"});
}

pattern_match_algo PatternMatchFactory::getPatternMatchAlgo(TermMatchType termMatchType) {
    switch (termMatchType) {
    case TermMatchType::EXACT:
        return exactTermMatch;
    case TermMatchType::STEM:
        return stemTermMatch;
    default:
        KU_UNREACHABLE;
    }
}

} // namespace fts_extension
} // namespace kuzu
