
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, CALL = 46, COMMENT = 47, MACRO = 48, GLOB = 49, COPY = 50, 
    FROM = 51, COLUMN = 52, NODE = 53, TABLE = 54, GROUP = 55, RDF = 56, 
    GRAPH = 57, DROP = 58, ALTER = 59, DEFAULT = 60, RENAME = 61, ADD = 62, 
    PRIMARY = 63, KEY = 64, REL = 65, TO = 66, EXPLAIN = 67, PROFILE = 68, 
    BEGIN = 69, TRANSACTION = 70, READ = 71, ONLY = 72, WRITE = 73, COMMIT = 74, 
    COMMIT_SKIP_CHECKPOINT = 75, ROLLBACK = 76, ROLLBACK_SKIP_CHECKPOINT = 77, 
    INSTALL = 78, EXTENSION = 79, UNION = 80, ALL = 81, LOAD = 82, HEADERS = 83, 
    OPTIONAL = 84, MATCH = 85, UNWIND = 86, CREATE = 87, MERGE = 88, ON = 89, 
    SET = 90, DETACH = 91, DELETE = 92, WITH = 93, RETURN = 94, DISTINCT = 95, 
    STAR = 96, AS = 97, ORDER = 98, BY = 99, L_SKIP = 100, LIMIT = 101, 
    ASCENDING = 102, ASC = 103, DESCENDING = 104, DESC = 105, WHERE = 106, 
    SHORTEST = 107, OR = 108, XOR = 109, AND = 110, NOT = 111, INVALID_NOT_EQUAL = 112, 
    MINUS = 113, FACTORIAL = 114, COLON = 115, IN = 116, STARTS = 117, ENDS = 118, 
    CONTAINS = 119, IS = 120, NULL_ = 121, TRUE = 122, FALSE = 123, COUNT = 124, 
    EXISTS = 125, CASE = 126, ELSE = 127, END = 128, WHEN = 129, THEN = 130, 
    StringLiteral = 131, EscapedChar = 132, DecimalInteger = 133, HexLetter = 134, 
    HexDigit = 135, Digit = 136, NonZeroDigit = 137, NonZeroOctDigit = 138, 
    ZeroDigit = 139, RegularDecimalReal = 140, UnescapedSymbolicName = 141, 
    IdentifierStart = 142, IdentifierPart = 143, EscapedSymbolicName = 144, 
    SP = 145, WHITESPACE = 146, Comment = 147, Unknown = 148
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

