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

    ~NodePattern() = default;

    inline string getName() const { return name; }

    inline string getLabel() const { return label; }

    bool operator==(const NodePattern& other) const {
        return name == other.name && label == other.label;
    }

    bool operator!=(const NodePattern& other) const { return !operator==(other); }

private:
    string name;
    string label;
};

} // namespace parser
} // namespace graphflow
