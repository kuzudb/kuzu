#pragma once

#include <memory>
#include <string>

using namespace std;

namespace graphflow {
namespace parser {

/**
 * NodePattern represents "(nodeName:NodeType)"
 */
class NodePattern {

public:
    NodePattern(string name, string label) : name{move(name)}, label{move(label)} {}

    bool operator==(const NodePattern& other) { return name == other.name && label == other.label; }

public:
    string name;
    string label;
};

} // namespace parser
} // namespace graphflow
