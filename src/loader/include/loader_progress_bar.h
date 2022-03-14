#pragma once

#include <algorithm>
#include <iostream>
#include <mutex>

using namespace std;

namespace graphflow {
namespace loader {

/**
 * Helper class to print progress bar on the console during loading. Its intended use is as follows:
 * Caller adds and starts a new {\it job} with some number of {\it tasks}.
 *
 * progressBar->addAndStartNewJob("Job X", 5); // in this example numTasks = 5
 *
 * After this he caller cannot start another job until this job is finished.
 * Possibly in parallel, e.g., by LoaderTasks, there should be exactly 5
 * progressBar->incrementTaskFinished(). Each time incrementTaskFinished is called, the progress bar
 * on the console would increase by 20% in this example.
 * Once the 5th call is made, the progress bar disappears and a "Done Job  X" appears.
 *
 * Warning: The caller should *NOT* log anything else on the console until the 5th
 * incrementTaskFinished call is made.
 *
 * The caller can then do one of the following:
 * (1) Start a new Job e.g.: progressBar->addAndStartNewJob("Job Y", 13); // now numTasks = 13
 * (2) Or clear the last "Done Job X" line by calling progressBar->clearLastFinishedLine() and print
 * anything else on the screen e.g.:
 *         progressBar->clearLastFinishedLine();
 *         logger->info("This is a print line not output through the LoaderProgressBar.")
 */
class LoaderProgressBar {

public:
    LoaderProgressBar()
        : numJobs{0}, numJobsFinished{0}, numTasksInCurrentJob{0}, numTasksFinishedInCurrentJob{-1},
          lastFinishedLineLenToClear{0} {};

    void addAndStartNewJob(string jobName, uint64_t numTasks);

    void incrementTaskFinished();

    inline void clearLastFinishedLine() const {
        string clearLine(lastFinishedLineLenToClear, ' ');
        cout << clearLine << "\r";
    }

    inline void setDefaultFont() const { cerr << "\033[0m"; }

    void printProgressInfoInGreenFont() const;

private:
    void setGreenFont() { cerr << "\033[1;32m"; }

private:
    int numJobs;
    int numJobsFinished;
    string currentJobName;
    int numTasksInCurrentJob;
    int numTasksFinishedInCurrentJob;
    uint64_t lastFinishedLineLenToClear;
    mutex progressBarLock;
};

} // namespace loader
} // namespace graphflow
