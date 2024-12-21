#pragma once

#include <string.h>

#include <memory>

#include "common/constants.h"
#include "common/types/types.h"
#include <span>

namespace kuzu {
namespace common {

class SelectionVector {
    KUZU_API static const std::array<sel_t, DEFAULT_VECTOR_CAPACITY> INCREMENTAL_SELECTED_POS;

    // In DYNAMIC mode, selectedPositions points to a mutable buffer that can be modified through
    // getMutableBuffer In STATIC mode, selectedPositions points to the beginning of
    // INCREMENTAL_SELECTED_POS In STATIC_FILTERED mode, selectedPositions points to some position
    // in INCREMENTAL_SELECTED_POS
    //      If reading manually in STATIC_FILTERED mode, you should read getSelectedPositions()[0],
    //      and then you can assume that the next getSelSize() positions are selected
    //      (This also works in STATIC mode)
    enum class State {
        DYNAMIC,
        STATIC,
    };

public:
    explicit SelectionVector(sel_t capacity)
        : selectedSize{0}, capacity{capacity}, selectedPositions{nullptr}, state{State::STATIC} {
        selectedPositionsBuffer = std::make_unique<sel_t[]>(capacity);
        setToUnfiltered();
    }

    SelectionVector() : SelectionVector{DEFAULT_VECTOR_CAPACITY} {}

    bool isUnfiltered() const { return state == State::STATIC && selectedPositions[0] == 0; }

    void setToUnfiltered() {
        selectedPositions = const_cast<sel_t*>(INCREMENTAL_SELECTED_POS.data());
        state = State::STATIC;
    }
    void setToUnfiltered(sel_t size) {
        KU_ASSERT(size <= capacity);
        selectedPositions = const_cast<sel_t*>(INCREMENTAL_SELECTED_POS.data());
        selectedSize = size;
        state = State::STATIC;
    }
    void setRange(sel_t startPos, sel_t size) {
        KU_ASSERT(startPos + size <= capacity);
        selectedPositions = selectedPositionsBuffer.get();
        for (auto i = 0u; i < size; ++i) {
            selectedPositions[i] = startPos + i;
        }
        selectedSize = size;
        state = State::DYNAMIC;
    }

    // Set to filtered is not very accurate. It sets selectedPositions to a mutable array.
    void setToFiltered() {
        selectedPositions = selectedPositionsBuffer.get();
        state = State::DYNAMIC;
    }
    void setToFiltered(sel_t size) {
        KU_ASSERT(size <= capacity && selectedPositionsBuffer);
        setToFiltered();
        selectedSize = size;
    }

    // Copies the data in selectedPositions into selectedPositionsBuffer
    void makeDynamic() {
        memcpy(selectedPositionsBuffer.get(), selectedPositions, selectedSize * sizeof(sel_t));
        state = State::DYNAMIC;
        selectedPositions = selectedPositionsBuffer.get();
    }

    std::span<sel_t> getMutableBuffer() const {
        return std::span<sel_t>(selectedPositionsBuffer.get(), capacity);
    }
    std::span<const sel_t> getSelectedPositions() const {
        return std::span<const sel_t>(selectedPositions, selectedSize);
    }

    template<class Func>
    void forEach(Func&& func) const {
        if (state == State::DYNAMIC) {
            for (size_t i = 0; i < selectedSize; i++) {
                func(selectedPositions[i]);
            }
        } else {
            const auto start = selectedPositions[0];
            for (size_t i = start; i < start + selectedSize; i++) {
                func(i);
            }
        }
    }

    sel_t getSelSize() const { return selectedSize; }
    void setSelSize(sel_t size) {
        KU_ASSERT(size <= capacity);
        selectedSize = size;
    }
    void incrementSelSize(sel_t increment = 1) {
        KU_ASSERT(selectedSize < capacity);
        selectedSize += increment;
    }

    sel_t operator[](sel_t index) const {
        KU_ASSERT(index < capacity);
        return selectedPositions[index];
    }
    sel_t& operator[](sel_t index) {
        KU_ASSERT(index < capacity);
        return selectedPositions[index];
    }

private:
    sel_t selectedSize;
    sel_t capacity;
    std::unique_ptr<sel_t[]> selectedPositionsBuffer;
    sel_t* selectedPositions;
    State state;
};

} // namespace common
} // namespace kuzu
