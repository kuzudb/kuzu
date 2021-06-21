
#include "tools/shell/include/embedded_shell.h"

using namespace std;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s [path to serialized dataset].\n", argv[0]);
        exit(1);
    }
    auto serializedGraphPath = argv[1];
    auto system = System(serializedGraphPath);
    auto shell = EmbeddedShell(system);
    shell.initialize();
    shell.run();

    return 0;
}
