#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class NodeIDMap {

public:
    NodeIDMap(uint64_t size)
        : nodeIDToOffsetMapping{unordered_map<string, gfNodeOffset_t>(size)} {};

    void setOffset(string nodeID, gfNodeOffset_t offset);
    gfNodeOffset_t getOffset(string &nodeID);
    bool hasNodeID(string &nodeID);
    void merge(NodeIDMap &localMap);

private:
    unordered_map<string, gfNodeOffset_t> nodeIDToOffsetMapping;
};

class InMemAdjEdges {

public:
    InMemAdjEdges(const string fname, uint64_t numElements, uint64_t numBytesPerLabel,
        uint64_t numBytesPerOffset);

    void set(gfNodeOffset_t offset, gfLabel_t nbrLabel, gfNodeOffset_t nbrOffset);

    void saveToFile();

private:
    unique_ptr<uint8_t[]> data;
    ssize_t size;
    const string fname;
    uint64_t numBytesPerLabel;
    uint64_t numBytesPerOffset;
    uint64_t numElementsPerPage;
};

class InMemAdjListsIndex {

public:
    InMemAdjListsIndex(const string fname, uint64_t numPages, uint64_t numBytesPerLabel,
        uint64_t numBytesPerOffset);

    void set(uint64_t pageIdx, uint16_t pageOffset, uint64_t pos, gfLabel_t nbrLabel,
        gfNodeOffset_t nbrOffset);

    void saveToFile();

private:
    unique_ptr<uint8_t[]> data;
    ssize_t size;
    const string fname;
    uint64_t numBytesPerLabel;
    uint64_t numBytesPerOffset;
    uint64_t numElementsPerPage;
};

} // namespace common
} // namespace graphflow
