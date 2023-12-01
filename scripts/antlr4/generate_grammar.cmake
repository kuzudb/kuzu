# Use a copy of the grammar file and compare since last run.
# This is to make sure clean build from source needn't have Java installed.
# We can't use checksums because of windows line ending normalization.
file(READ Cypher.g4.copy COPY_CONTENT)
file(READ ${ROOT_DIR}/src/antlr4/Cypher.g4 REAL_CONTENT)

if("${COPY_CONTENT}" STREQUAL "${REAL_CONTENT}")
    message(DEBUG " Not regenerating grammar files as Cypher.g4 is unchanged.")
    return() # Exit.
endif()

file(WRITE Cypher.g4.copy "${REAL_CONTENT}")

message(INFO " Regenerating grammar files...")

if(NOT EXISTS antlr4.jar)
    message(INFO " Downloading antlr4.jar")
    file(
        DOWNLOAD https://www.antlr.org/download/antlr-4.13.1-complete.jar antlr4.jar
        EXPECTED_HASH SHA256=bc13a9c57a8dd7d5196888211e5ede657cb64a3ce968608697e4f668251a8487)
endif()

# create the directory for the generated grammar
file(MAKE_DIRECTORY generated)

# copy grammar, because antlr4 does some weird things with output locations.
file(COPY ${ROOT_DIR}/src/antlr4/Cypher.g4 DESTINATION .)

find_package(Java REQUIRED)

# Generate files.
message(INFO " Generating files...")
execute_process(COMMAND
    ${Java_JAVA_EXECUTABLE} -jar antlr4.jar -Dlanguage=Cpp -no-visitor -no-listener Cypher.g4 -o generated)

# Edit source files.
file(READ generated/CypherLexer.cpp LexerContent)
string(REPLACE "#include \"CypherLexer.h\"" "#include \"cypher_lexer.h\"" LexerReplacedContent "${LexerContent}")
file(WRITE ${ROOT_DIR}/third_party/antlr4_cypher/cypher_lexer.cpp "${LexerReplacedContent}")

file(READ generated/CypherParser.cpp ParserContent)
string(REPLACE "#include \"CypherParser.h\"" "#include \"cypher_parser.h\"" ParserReplacedContent "${ParserContent}")
file(WRITE ${ROOT_DIR}/third_party/antlr4_cypher/cypher_parser.cpp "${ParserReplacedContent}")

# Move headers.
file(RENAME generated/CypherParser.h ${ROOT_DIR}/third_party/antlr4_cypher/include/cypher_parser.h)
file(RENAME generated/CypherLexer.h ${ROOT_DIR}/third_party/antlr4_cypher/include/cypher_lexer.h)

# Cleanup
file(REMOVE_RECURSE generated)
file(REMOVE Cypher.g4)
