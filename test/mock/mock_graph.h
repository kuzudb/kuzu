#include "gmock/gmock.h"
#include "test/mock/mock_catalog.h"

#include "src/storage/include/graph.h"

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::ReturnRef;
using ::testing::StrEq;
using ::testing::Throw;

using namespace graphflow::storage;

class MockGraph : public Graph {

public:
    MOCK_METHOD(const Catalog&, getCatalog, (), (const, override));
    MOCK_METHOD(uint64_t, getNumNodes, (label_t label), (const, override));
    MOCK_METHOD(uint64_t, getNumRelsForDirBoundLabelRelLabel,
        (Direction direction, label_t boundNodeLabel, label_t relLabel), (const, override));
};

/**
 * Mock person-knows-person graph with 10000 person nodes
 * fwd average degree 10, bwd average degree 20
 */
class PersonKnowsPersonGraph : public MockGraph {

public:
    void setUp() {
        numPersonNodes = 10000;
        setCatalog();

        setActionForGetCatalog();
        setActionForGetNumNodes();
        setActionForGetNumRelsForDirBoundLabelRelLabel();
    }

private:
    void setActionForGetCatalog() { ON_CALL(*this, getCatalog).WillByDefault(ReturnRef(*catalog)); }

    void setActionForGetNumNodes() {
        ON_CALL(*this, getNumNodes(_))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getNumNodes(0)).WillByDefault(Return(numPersonNodes));
    }

    void setActionForGetNumRelsForDirBoundLabelRelLabel() {
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(_, _, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(FWD, 0, 0))
            .WillByDefault(Return(10 * numPersonNodes));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(BWD, 0, 0))
            .WillByDefault(Return(20 * numPersonNodes));
    }

    void setCatalog() {
        auto mockCatalog = make_unique<NiceMock<PersonKnowsPersonCatalog>>();
        mockCatalog->setUp();
        catalog = move(mockCatalog);
    }

    unique_ptr<Catalog> catalog;
    uint64_t numPersonNodes;
};
