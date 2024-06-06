#pragma once

#include <string>
#include <vector>

namespace kuzu {
namespace parser {

// NOTE: keep this class in the root of parser module in case we make it a statement in the future.
class ProjectGraph {
public:
    ProjectGraph(std::string graphName, std::vector<std::string> tableNames)
        : graphName{std::move(graphName)}, tableNames{std::move(tableNames)} {}

    std::string getGraphName() const { return graphName; }
    std::vector<std::string> getTableNames() const { return tableNames; }

private:
    std::string graphName;
    std::vector<std::string> tableNames;
};

} // namespace parser
} // namespace kuzu
