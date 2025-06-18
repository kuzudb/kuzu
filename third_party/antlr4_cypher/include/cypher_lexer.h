
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
    T__44 = 45, ACYCLIC = 46, ANY = 47, ADD = 48, ALL = 49, ALTER = 50, 
    AND = 51, AS = 52, ASC = 53, ASCENDING = 54, ATTACH = 55, BEGIN = 56, 
    BY = 57, CALL = 58, CASE = 59, CAST = 60, CHECKPOINT = 61, COLUMN = 62, 
    COMMENT = 63, COMMIT = 64, COMMIT_SKIP_CHECKPOINT = 65, CONTAINS = 66, 
    COPY = 67, COUNT = 68, CREATE = 69, CYCLE = 70, DATABASE = 71, DBTYPE = 72, 
    DEFAULT = 73, DELETE = 74, DESC = 75, DESCENDING = 76, DETACH = 77, 
    DISTINCT = 78, DROP = 79, ELSE = 80, END = 81, ENDS = 82, EXISTS = 83, 
    EXPLAIN = 84, EXPORT = 85, EXTENSION = 86, FALSE = 87, FROM = 88, FORCE = 89, 
    GLOB = 90, GRAPH = 91, GROUP = 92, HEADERS = 93, HINT = 94, IMPORT = 95, 
    IF = 96, IN = 97, INCREMENT = 98, INSTALL = 99, IS = 100, JOIN = 101, 
    KEY = 102, LIMIT = 103, LOAD = 104, LOGICAL = 105, MACRO = 106, MATCH = 107, 
    MAXVALUE = 108, MERGE = 109, MINVALUE = 110, MULTI_JOIN = 111, NO = 112, 
    NODE = 113, NOT = 114, NONE = 115, NULL_ = 116, ON = 117, ONLY = 118, 
    OPTIONAL = 119, OR = 120, ORDER = 121, PRIMARY = 122, PROFILE = 123, 
    PROJECT = 124, READ = 125, REL = 126, RENAME = 127, RETURN = 128, ROLLBACK = 129, 
    ROLLBACK_SKIP_CHECKPOINT = 130, SEQUENCE = 131, SET = 132, SHORTEST = 133, 
    START = 134, STARTS = 135, TABLE = 136, THEN = 137, TO = 138, TRAIL = 139, 
    TRANSACTION = 140, TRUE = 141, TYPE = 142, UNION = 143, UNWIND = 144, 
    UNINSTALL = 145, UPDATE = 146, USE = 147, WHEN = 148, WHERE = 149, WITH = 150, 
    WRITE = 151, WSHORTEST = 152, XOR = 153, SINGLE = 154, YIELD = 155, 
    DECIMAL = 156, STAR = 157, L_SKIP = 158, INVALID_NOT_EQUAL = 159, COLON = 160, 
    MINUS = 161, FACTORIAL = 162, StringLiteral = 163, EscapedChar = 164, 
    DecimalInteger = 165, HexLetter = 166, HexDigit = 167, Digit = 168, 
    NonZeroDigit = 169, NonZeroOctDigit = 170, ZeroDigit = 171, ExponentDecimalReal = 172, 
    RegularDecimalReal = 173, UnescapedSymbolicName = 174, IdentifierStart = 175, 
    IdentifierPart = 176, EscapedSymbolicName = 177, SP = 178, WHITESPACE = 179, 
    CypherComment = 180, Unknown = 181
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

