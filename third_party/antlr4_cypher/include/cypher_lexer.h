
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
    T__44 = 45, ATTACH = 46, DBTYPE = 47, CALL = 48, COMMENT_ = 49, MACRO = 50, 
    GLOB = 51, COPY = 52, FROM = 53, COLUMN = 54, EXPORT = 55, IMPORT = 56, 
    DATABASE = 57, NODE = 58, TABLE = 59, GROUP = 60, RDFGRAPH = 61, DROP = 62, 
    ALTER = 63, DEFAULT = 64, RENAME = 65, ADD = 66, PRIMARY = 67, KEY = 68, 
    REL = 69, TO = 70, EXPLAIN = 71, PROFILE = 72, BEGIN = 73, TRANSACTION = 74, 
    READ = 75, ONLY = 76, WRITE = 77, COMMIT = 78, COMMIT_SKIP_CHECKPOINT = 79, 
    ROLLBACK = 80, ROLLBACK_SKIP_CHECKPOINT = 81, INSTALL = 82, EXTENSION = 83, 
    UNION = 84, ALL = 85, LOAD = 86, HEADERS = 87, OPTIONAL = 88, MATCH = 89, 
    UNWIND = 90, CREATE = 91, MERGE = 92, ON = 93, SET = 94, DETACH = 95, 
    DELETE = 96, WITH = 97, RETURN = 98, DISTINCT = 99, STAR = 100, AS = 101, 
    ORDER = 102, BY = 103, L_SKIP = 104, LIMIT = 105, ASCENDING = 106, ASC = 107, 
    DESCENDING = 108, DESC = 109, WHERE = 110, SHORTEST = 111, OR = 112, 
    XOR = 113, AND = 114, NOT = 115, INVALID_NOT_EQUAL = 116, MINUS = 117, 
    FACTORIAL = 118, COLON = 119, IN = 120, STARTS = 121, ENDS = 122, CONTAINS = 123, 
    IS = 124, NULL_ = 125, TRUE = 126, FALSE = 127, COUNT = 128, EXISTS = 129, 
    CASE = 130, ELSE = 131, END = 132, WHEN = 133, THEN = 134, StringLiteral = 135, 
    EscapedChar = 136, DecimalInteger = 137, HexLetter = 138, HexDigit = 139, 
    Digit = 140, NonZeroDigit = 141, NonZeroOctDigit = 142, ZeroDigit = 143, 
    RegularDecimalReal = 144, UnescapedSymbolicName = 145, IdentifierStart = 146, 
    IdentifierPart = 147, EscapedSymbolicName = 148, SP = 149, WHITESPACE = 150, 
    Comment = 151, Unknown = 152
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

