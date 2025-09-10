#pragma once

#include "graph/graph.h"
#include "processor/execution_context.h"
#include "re2.h"

namespace kuzu {
namespace fts_extension {

struct FTSConfig;
struct QueryFTSBindData;

using VCQueryTerm = std::variant<std::string, std::unique_ptr<RE2>>;

using pattern_match_algo = std::function<void(std::unordered_map<common::offset_t, uint64_t>& dfs,
    std::vector<VCQueryTerm>& vcQueryTerms, processor::ExecutionContext* executionContext,
    graph::Graph* graph, const QueryFTSBindData& bindData)>;

class PatternMatchFactory {
public:
    static pattern_match_algo getPatternMatchAlgo(bool isAdvanced);
};

} // namespace fts_extension
} // namespace kuzu
