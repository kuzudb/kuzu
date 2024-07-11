#pragma once

#include "graph.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace graph {

/*
 * Change this depending on whatever is the size of each morsel CSR.
 */
#define MORSEL_SIZE 2048
#define RIGHT_SHIFT (uint64_t) log2(MORSEL_SIZE)
#define OFFSET_DIV 0x7FF

struct MorselCSR {
    uint64_t csr_v[MORSEL_SIZE + 1]{0};
    common::offset_t* nbrNodeOffsets;
    uint64_t blockSize;

    explicit MorselCSR(uint64_t size) {
        nbrNodeOffsets = new common::offset_t[size];
        blockSize = size;
    }

    ~MorselCSR() { delete[] nbrNodeOffsets; }
};

struct CSRIndexSharedState {
    std::vector<MorselCSR*> csr; // stores a pointer to the CSREntry struct

    ~CSRIndexSharedState() {
        auto duration1 = std::chrono::system_clock::now().time_since_epoch();
        auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
        printf("starting to release back memory ...\n");
        for (auto& entry : csr) {
            delete entry;
        }
        auto duration2 = std::chrono::system_clock::now().time_since_epoch();
        auto millis2 = std::chrono::duration_cast<std::chrono::milliseconds>(duration2).count();
        printf("time taken to free memory %lu ms\n", millis2 - millis1);
    }
};

class InMemGraph : public Graph {
public:
    InMemGraph(main::ClientContext* context, common::table_id_t nodeTableID,
        common::table_id_t relTableID, std::shared_ptr<CSRIndexSharedState> csrSharedState);

    inline std::vector<MorselCSR*>& getInMemCSR() {
        return csrSharedState->csr;
    }

    common::table_id_t getNodeTableID() override { return nodeTable->getTableID(); }

    common::offset_t getNumNodes() override;

    common::offset_t getNumEdges() override;

    std::vector<common::nodeID_t> getNbrs(common::offset_t offset,
        NbrScanState* nbrScanState) override;

    void initializeStateFwdNbrs(common::offset_t offset, NbrScanState* nbrScanState) override;

    bool hasMoreFwdNbrs(NbrScanState* nbrScanState) override;

    void getFwdNbrs(NbrScanState* nbrScanState) override;

private:
    main::ClientContext* context;
    storage::NodeTable* nodeTable;
    storage::RelTable* relTable;
    std::shared_ptr<CSRIndexSharedState> csrSharedState;
};

} // namespace graph
} // namespace kuzu

