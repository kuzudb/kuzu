#pragma once

#include <string>
#include <utility>

using namespace std;

namespace graphflow {
namespace planner {

const int ANY_LABEL = -1;

class QueryNode {

public:
    QueryNode(string name, int label) : name{move(name)}, label{label} {}

    string getName() const { return name; }

    int getLabel() const { return label; }

    bool operator==(const QueryNode& other) const {
        return name == other.name && label == other.label;
    }

private:
    string name;
    int label;
};

} // namespace planner
} // namespace graphflow
