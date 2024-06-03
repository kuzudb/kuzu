#pragma once

#include <iostream>

#include "progress_bar_display.h"

namespace kuzu {
namespace common {

/**
 * @brief A class that displays a progress bar in the terminal.
 */
class TerminalProgressBarDisplay final : public ProgressBarDisplay {
public:
    void updateProgress(double newPipelineProgress, uint32_t newNumPipelinesFinished) override;

    void finishProgress() override;

private:
    void setGreenFont() const { std::cerr << "\033[1;32m"; }

    void setDefaultFont() const { std::cerr << "\033[0m"; }

    void printProgressBar();

private:
    bool printing = false;
};

} // namespace common
} // namespace kuzu
