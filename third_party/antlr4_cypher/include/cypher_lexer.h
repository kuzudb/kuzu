
// Generated from Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, MACRO = 49, GLOB = 50, 
    COPY = 51, FROM = 52, NPY = 53, COLUMN = 54, NODE = 55, TABLE = 56, 
    DROP = 57, ALTER = 58, DEFAULT = 59, RENAME = 60, ADD = 61, PRIMARY = 62, 
    KEY = 63, REL = 64, TO = 65, EXPLAIN = 66, PROFILE = 67, UNION = 68, 
    ALL = 69, OPTIONAL = 70, MATCH = 71, UNWIND = 72, CREATE = 73, SET = 74, 
    DELETE = 75, WITH = 76, RETURN = 77, DISTINCT = 78, STAR = 79, AS = 80, 
    ORDER = 81, BY = 82, L_SKIP = 83, LIMIT = 84, ASCENDING = 85, ASC = 86, 
    DESCENDING = 87, DESC = 88, WHERE = 89, SHORTEST = 90, OR = 91, XOR = 92, 
    AND = 93, NOT = 94, INVALID_NOT_EQUAL = 95, MINUS = 96, FACTORIAL = 97, 
    STARTS = 98, ENDS = 99, CONTAINS = 100, IS = 101, NULL_ = 102, TRUE = 103, 
    FALSE = 104, EXISTS = 105, CASE = 106, ELSE = 107, END = 108, WHEN = 109, 
    THEN = 110, StringLiteral = 111, EscapedChar = 112, DecimalInteger = 113, 
    HexLetter = 114, HexDigit = 115, Digit = 116, NonZeroDigit = 117, NonZeroOctDigit = 118, 
    ZeroDigit = 119, RegularDecimalReal = 120, UnescapedSymbolicName = 121, 
    IdentifierStart = 122, IdentifierPart = 123, EscapedSymbolicName = 124, 
    SP = 125, WHITESPACE = 126, Comment = 127, Unknown = 128
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

