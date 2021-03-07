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
    void setName(const string& name) { this->name = name; }

    void setLabel(const string& label) { this->label = label; }

    bool operator==(const NodePattern& other) { return name == other.name && label == other.label; }

private:
    string name;
    string label;
};

} // namespace parser
} // namespace graphflow
