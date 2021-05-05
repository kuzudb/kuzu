#include <memory>

#include "args.hxx"

#include "src/main/include/server.h"

using namespace std;
using namespace graphflow::main;

int main(int argc, char* argv[]) {
    args::ArgumentParser parser("Runs the GraphflowDB Server.");
    args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
    args::ValueFlag<string> hostArg(parser, "host", "Host IP or URL", {'l', "host"});
    args::ValueFlag<int> portArg(parser, "port", "port", {'p', "port"});
    try {
        parser.ParseCLI(argc, argv);
    } catch (const args::Help&) {
        cout << parser;
        return 0;
    } catch (const args::ParseError& e) {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    } catch (const args::ValidationError& e) {
        cerr << e.what() << std::endl;
        cerr << parser;
        return 1;
    }
    Server().run(hostArg ? args::get(hostArg) : "localhost", portArg ? args::get(portArg) : 3110);
}
