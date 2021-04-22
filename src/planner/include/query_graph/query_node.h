#pragma once

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace planner {

const label_t ANY_LABEL = numeric_limits<uint32_t>::max();

class QueryNode {

public:
    QueryNode(string name, label_t label) : name{move(name)}, label{label} {}

public:
    string name;
    label_t label;
};

} // namespace planner
} // namespace graphflow
