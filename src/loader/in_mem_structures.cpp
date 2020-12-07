#include "src/loader/include/in_mem_structures.h"

#include <fcntl.h>
#include <string.h>
#include <unistd.h>

namespace graphflow {
namespace loader {

void InMemStructure::saveToFile() {
    uint32_t f = open(fname.c_str(), O_WRONLY | O_CREAT, 0666);
    if (-1u == f) {
        invalid_argument("cannot create file: " + fname);
    }
    if (size != write(f, data.get(), size)) {
        invalid_argument("Cannot write in file.");
    }
    close(f);
}

InMemAdjRels::InMemAdjRels(
    const string fname, uint64_t numElements, uint8_t numBytesPerLabel, uint8_t numBytesPerOffset)
    : InMemAdjStructure{fname, numBytesPerLabel, numBytesPerOffset} {
    size = PAGE_SIZE * (numElements / numElementsPerPage);
    if (0 != numElements % numElementsPerPage) {
        size += PAGE_SIZE;
    }
    initData();
};

void InMemAdjRels::set(node_offset_t offset, label_t nbrLabel, node_offset_t nbrOffset) {
    auto writeOffset = data.get() + (PAGE_SIZE * (offset / numElementsPerPage)) +
                       ((numBytesPerLabel + numBytesPerOffset) * (offset % numElementsPerPage));
    memcpy(writeOffset, &nbrLabel, numBytesPerLabel);
    memcpy(writeOffset + numBytesPerLabel, &nbrOffset, numBytesPerOffset);
}

InMemAdjLists::InMemAdjLists(
    const string fname, uint64_t numPages, uint8_t numBytesPerLabel, uint8_t numBytesPerOffset)
    : InMemAdjStructure{fname, numBytesPerLabel, numBytesPerOffset} {
    size = PAGE_SIZE * numPages;
    initData();
};

void InMemAdjLists::set(
    uint64_t pageIdx, uint16_t pageOffset, label_t nbrLabel, node_offset_t nbrOffset) {
    auto writeOffset =
        data.get() + (PAGE_SIZE * pageIdx) + ((numBytesPerLabel + numBytesPerOffset) * pageOffset);
    memcpy(writeOffset, &nbrLabel, numBytesPerLabel);
    memcpy(writeOffset + numBytesPerLabel, &nbrOffset, numBytesPerOffset);
}

InMemPropertyColumn::InMemPropertyColumn(
    const string fname, uint64_t numElements, uint8_t numBytesPerElement)
    : InMemPropertyStructure{fname, numBytesPerElement} {
    numElementsPerPage = PAGE_SIZE / numBytesPerElement;
    size = PAGE_SIZE * (numElements / numElementsPerPage);
    if (0 != numElements % numElementsPerPage) {
        size += PAGE_SIZE;
    }
    initData();
};

void InMemPropertyColumn::set(node_offset_t offset, uint8_t* val) {
    // WARNING: Assuming numBytesPerElement to be power of 2.
    // Under this assumption, PAGE_SIZE / numElementsPerPage = numBytesPerElement
    auto writeOffset = data.get() + (numBytesPerElement * offset) +
                       (numBytesPerElement * (offset % numElementsPerPage));
    memcpy(writeOffset, val, numBytesPerElement);
}

InMemPropertyLists::InMemPropertyLists(
    const string fname, uint64_t numPages, uint64_t numBytesPerElement)
    : InMemPropertyStructure{fname, numBytesPerElement} {
    size = PAGE_SIZE * numPages;
    initData();
};

void InMemPropertyLists::set(uint64_t pageIdx, uint16_t pageOffset, uint8_t* val) {
    auto writeOffset = data.get() + (PAGE_SIZE * pageIdx) + (numBytesPerElement * pageOffset);
    memcpy(writeOffset, val, numBytesPerElement);
}

} // namespace loader
} // namespace graphflow
