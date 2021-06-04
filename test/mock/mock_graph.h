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
 * Mock tiny snb graph with 10000 person nodes and 100 organisation nodes
 * knows edge has fwd average degree 10, bwd average degree 20
 * workAt edge has fwd average degree 1 (column extend), bwd average degree 100
 */
class TinySnbGraph : public MockGraph {

public:
    void setUp() {
        numPersonNodes = 10000;
        numOrganisationNodes = 100;
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
        ON_CALL(*this, getNumNodes(1)).WillByDefault(Return(numOrganisationNodes));
    }

    void setActionForGetNumRelsForDirBoundLabelRelLabel() {
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(_, _, _))
            .WillByDefault(Throw(invalid_argument("Should never happen.")));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(FWD, 0, 0))
            .WillByDefault(Return(10 * numPersonNodes));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(BWD, 0, 0))
            .WillByDefault(Return(20 * numPersonNodes));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(FWD, 0, 1))
            .WillByDefault(Return(1 * numPersonNodes));
        ON_CALL(*this, getNumRelsForDirBoundLabelRelLabel(BWD, 1, 1))
            .WillByDefault(Return(100 * numOrganisationNodes));
    }

    void setCatalog() {
        auto mockCatalog = make_unique<NiceMock<TinySnbCatalog>>();
        mockCatalog->setUp();
        catalog = move(mockCatalog);
    }

    unique_ptr<Catalog> catalog;
    uint64_t numPersonNodes;
    uint64_t numOrganisationNodes;
};
