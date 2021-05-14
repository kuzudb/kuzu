#include "gmock/gmock.h"

#include "src/storage/include/graph.h"

using namespace graphflow::storage;

class MockGraph : public Graph {

public:
    MOCK_METHOD(const Catalog&, getCatalog, (), (const, override));
    MOCK_METHOD(uint64_t, getNumNodes, (label_t label), (const, override));
    MOCK_METHOD(uint64_t, getNumRelsForDirBoundLabelRelLabel,
        (Direction direction, label_t boundNodeLabel, label_t relLabel), (const, override));
};
