#pragma once

#include <memory>

#include "src/common/include/configs.h"
#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace loader {

// In-memory Structure(s) holds the Column/Lists data in memory during rels loading.
class InMemStructure {

public:
    InMemStructure(const string fname) : fname{fname} {};

    inline void initData() {
        data = make_unique<uint8_t[]>(size);
        fill(data.get(), data.get() + size, UINT8_MAX);
    }

    void saveToFile();

protected:
    unique_ptr<uint8_t[]> data;
    ssize_t size;
    const string fname;
};

// In-memory AdjStructure(s) holds the AdjLists/AdjRels data in memory during rels loading.
class InMemAdjStructure : public InMemStructure {

public:
    InMemAdjStructure(const string fname, uint8_t numBytesPerLabel, uint8_t numBytesPerOffset)
        : InMemStructure{fname}, numBytesPerLabel{numBytesPerLabel},
          numBytesPerOffset(numBytesPerOffset) {
        numElementsPerPage = PAGE_SIZE / (numBytesPerLabel + numBytesPerOffset);
    }

protected:
    uint8_t numBytesPerLabel;
    uint8_t numBytesPerOffset;
    uint32_t numElementsPerPage;
};

class InMemAdjRels : public InMemAdjStructure {

public:
    InMemAdjRels(const string fname, uint64_t numElements, uint8_t numBytesPerLabel,
        uint8_t numBytesPerOffset);

    void set(node_offset_t offset, label_t nbrLabel, node_offset_t nbrOffset);
};

class InMemAdjLists : public InMemAdjStructure {

public:
    InMemAdjLists(
        const string fname, uint64_t numPages, uint8_t numBytesPerLabel, uint8_t numBytesPerOffset);

    void set(uint64_t pageIdx, uint16_t pageOffset, label_t nbrLabel, node_offset_t nbrOffset);
};

// In-memory Structure(s) holds the PropertyColumn/PropertyList data in memory during rels loading.
class InMemPropertyStructure : public InMemStructure {

public:
    InMemPropertyStructure(const string fname, uint8_t numBytesPerElement)
        : InMemStructure{fname}, numBytesPerElement{numBytesPerElement} {
        numElementsPerPage = PAGE_SIZE / numBytesPerElement;
    };

protected:
    uint8_t numBytesPerElement;
    uint64_t numElementsPerPage;
};

class InMemPropertyColumn : public InMemPropertyStructure {

public:
    InMemPropertyColumn(const string fname, uint64_t numElements, uint8_t numBytesPerElement);

    void set(node_offset_t offset, uint8_t* val);
};

class InMemPropertyLists : public InMemPropertyStructure {

public:
    InMemPropertyLists(const string fname, uint64_t numPages, uint64_t numBytesPerElement);

    void set(uint64_t pageIdx, uint16_t pageOffset, uint8_t* val);
};

} // namespace loader
} // namespace graphflow
