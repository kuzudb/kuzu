#ifndef GRAPHFLOW_COMMON_LOADER_UTILS_H_
#define GRAPHFLOW_COMMON_LOADER_UTILS_H_

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

class InMemColAdjList {

public:
    InMemColAdjList(
        const string fname, uint64_t numElements, uint64_t maxLabelToFit, uint64_t maxOffsetToFit);

    void set(gfNodeOffset_t offset, gfLabel_t nbrLabel, gfNodeOffset_t nbrOffset);    
    void saveToFile();

public:
    unique_ptr<uint8_t[]> data;
    size_t size;

private:
    const string fname;
    uint64_t numElements;
    uint64_t numLabelBytes;
    uint64_t numOffsetBytes;
    uint64_t numElementsPerPage;
};

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_LOADER_UTILS_H_
