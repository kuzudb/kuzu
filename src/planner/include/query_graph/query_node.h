#pragma once

#include <string>
#include <utility>

#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

const label_t ANY_LABEL = numeric_limits<uint32_t>::max();

class QueryNode {

public:
    QueryNode(string name, label_t label) : name{move(name)}, label{label} {}

    bool operator==(const QueryNode& other) const {
        return name == other.name && label == other.label;
    }

public:
    string name;
    label_t label;
};

} // namespace planner
} // namespace graphflow
