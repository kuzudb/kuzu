# Shell Development Guide
### Shell Runner
The `shell_runner.cpp` file is the starting point for the shell program. This file handles the different flags for the shell and prepares the shell for running. 

For the shell flags, we use this c++ [args](https://taywee.github.io/args/) library. This allows us to easily define different types of flags. The main types we use are `Positional`, `HelpFlag`, `ValueFlag` and `Flag`. 
```c++
args::ArgumentParser  parser("KuzuDB Shell");
args::Positional<std::string> inputDirFlag(parser, "databasePath",
"Path to the database. If not given or set to \":memory:\", the database will be opened "
"under in-memory mode.");
args::HelpFlag  help(parser, "help", "Display this help menu", {'h', "help"});
args::ValueFlag<uint64_t> bpSizeInMBFlag(parser, "",
"Size of buffer pool for default and large page sizes in megabytes",
{'d', "default_bp_size", "defaultbpsize"}, -1u);
args::Flag  disableCompression(parser, "no_compression", "Disable compression",
{"no_compression", "nocompression"});
```
To add another flag, pick the type of flag that is needed for the use case and give it a name. The parameters that are needed are the `ArgumentParser` shown above, a name for the help menu, the description for the help menu, and the strings the user can type to specify the flag. 

All flag strings are parsed to be lower case so make sure there are no capital letters in the flag strings like `{"no_compression", "nocompression"}`. As a convention, if the flag string is multiple words, we have a snake_case flag and a flag with no spaces as seen above. 

If the flag is a value flag, you can receive the value by using `args::get()` with the flag as a parameter. You can then handle the flag as needed.

After the flags are parsed, the database and connection are set up, and an `EmbeddedShell` object is created. Using this object, we can process every line in the run command startup file (usually `.kuzurc`) and then start up the shell, calling `shell.run();`

### Embedded Shell
The `embedded_shell.cpp` file contains all the code to process completed queries. It handles the processing of shell `:` commands and processes and prints the query results to the terminal. 

#### Shell Configuration
The shell in configured using the `ShellConfig` struct. This struct is initialized to the default values for each shell setting. This struct is edited by `shell_runner.cpp` and then used when initializing the `EmbeddedShell` class. If a new shell setting is being added, make sure a default value is assigned in this struct.
```c++
struct  ShellConfig {
	const  char* path_to_history = "";
	uint64_t  maxRowSize = defaultMaxRows;
	uint32_t  maxPrintWidth = 0;
	std::unique_ptr<Printer> printer = std::make_unique<BoxPrinter>();
	bool  stats = true;
};
```

#### Shell Running
The `run()` function contains the loop for the shell program. This is the heart of the cli. It first turns off shell echo to make sure printing does not get ruined by user input. The input handling is handled by the `linenoise` library so the `linenoise` function is called to get the next line for processing. 

#### CTRL C Processing
When processing the input received by linenoise, we first trim the whitespace off of the beginning of the input, then check if `ctrl c` was pressed. A `ctrl_c`	 input in linenoise is returned as either `"\3\0"` or `"\3\3\0"`. The string `"\3\3\0"` indicates `ctrl c ` was pressed to clear the current line to go to a fresh line.  The `"\3\0"` indicates `ctrl c` was pressed on an empty line. This distinction is important because we exit the shell on concurrent empty line `ctrl c` presses.

If `ctrl c` is pressed outside of linenoise, we process it using the `interruptHandler()` function defined in `EmbeddedShell.cpp`. Currently, this only quits the shell, so more work is needed on handling this case. 

#### Shell Command Processing
After the `ctrl_c` check is done, we check if the input is a shell command. Shell commands start with a `:` and if the input starts with a `:` we can send it to the `processShellCommands()` function. Shell commands are marked in the shell command array defined near the top of the file. 
```c++
// build-in shell command
struct ShellCommand {
    const char* HELP = ":help";
    const char* CLEAR = ":clear";
    const char* QUIT = ":quit";
    const char* MAX_ROWS = ":max_rows";
    const char* MAX_WIDTH = ":max_width";
    const char* MODE = ":mode";
    const char* STATS = ":stats";
    const char* MULTI = ":multiline";
    const char* SINGLE = ":singleline";
    const char* HIGHLIGHT = ":highlight";
    const char* ERRORS = ":render_errors";
    const std::array<const char*, 11> commandList = {HELP, CLEAR, QUIT, MAX_ROWS, MAX_WIDTH, MODE,
        STATS, MULTI, SINGLE, HIGHLIGHT, ERRORS};
} shellCommand;
```
The `processShellCommands()` matches the first word in the input with its corresponding command and calls the function which handles that command. The second word can be used as the argument. If no command is matched, an error is thrown, using the `damerauLevenshteinDistance()` function to find the closest command to the input.

#### Processing Queries
If the input received by `linenoise` does not begin with `:`, the input is then checked to see if the query is complete using the `cypherComplete()` function. This function checks if quotes and brackets are complete, and that the last non-whitespace character is a semicolon. Then, it processes every query in the line by finding each semi colon (this can be improved) and adds the result to the query results vector. If the line is not a complete cypher statement, then we save the input to be added in front of the next input returned by `linenoise`. When the input is finished being processed, we update the table name vectors for auto completion purposes in the shell, print the results, and add the query to the `linenoise` history.

#### Printing Results
The error message printing for the shell is simple. If the query was not successful, we print the error provided by the query result using `queryResult.getErrorMessage()`. However, if the query is only one word long, chances are the user was trying to type a shell command, so we use the `damerauLevenshteinDistance()` function again and also inform the user the closest shell command to their query.

For printing successful query results, there are currently 13 different modes. These output modes are defined in the `output.h` file. Each output mode is defined in the `PrinterType` enum and have a `Printer` struct associated with that type which contain all the different symbols used to create the output.

If you are adding a new output mode, the following is needed:

- `PrinterType`
- `PrinterType` added to struct  `PrinterTypeUtils::fromString()`
- `Printer` struct with needed characters
- Mode added to `printModeInfo()`
- Case added to `setMode()`
- If table truncation is required, add it to the `isTableType()` function
- Printing support added to `printExecutionResult()`

Most printing modes are straightforward. They print the column names and the column results in the desired format. There are query statistics such as table size and execution time printed as well. The complicated output mode is the table drawing modes which require truncation. 

For table truncation, it starts off with calculating the max size for each column. It first checks every column name/type and then iterates over every tuple looking for the maximum size of that column. 

Once the max size of each column is needed, it is time to figure out the optimal size to truncate each column. First, the maximum width of the table is determined using the size of the terminal or the display width set via shell commands. This is the `sumGoal` value. Then, while the sum of the widths of all columns is greater than the `sumGoal`, the column widths are reduced in the following pattern:

- If the longest column can be reduced where it stays the longest column and the sumGoal is met, reduce that column. No column should be reduced smaller than the minimum column truncated size.
- Otherwise, reduce the column so it is the same length as the second smallest column. (Again, not smaller than the min size)
- Repeat those steps until the sum goal is met, reducing the largest columns always to the same values, if the columns have the same size, they should stay the same size.
- Once the columns have been truncated as much as possible and the current sum is still larger than the sum goal, it is time to truncate out columns.
- The columns are iterated over on both the left and the right, calculating the sum as it iterates. If the current column would put the sum over the `sumGoal`, the other iterator is paused and the current iterator removes all columns until it reaches the second iterator. 
- The first and last columns are never removed and will always be included in the sumGoal

Once the columns are successfully truncated, the column names, datatypes, and tuples can all be printed. The iterators from column truncation are utilized to print columns up to the truncation and after. As well, if there are more rows than the `max_rows` value, the middle rows are truncated out until the `max_rows` value would be met.

### Linenoise 
The `linenoise.cpp` file contains all the code for processing and displaying the input the user types. This is a third part library that has been heavily edited to fit the needs of our shell. This code starts off with setting the terminal to a raw mode which allows the `linenoise` code to directly process inputs such as `ctrl c`. There are two modes for the shell that affects `linenoise`: `:singleline` and `:multiline`. As `:singleline`was the original mode, the `linenoise` description will first be based off of that mode, and then the differences with `:multiline` will be mentioned.

#### Edit
The main function for `linenoise` is the `linenoiseEdit()` function. This contains the loop the processes every character inputted by the user and sets up the `linenoise` state. The loop contains a switch statement for each character that requires special processing and directs them to the correct function. 

After every character is processed, the `refreshLine()` function is called to update what the user sees on the screen. For `:singleline`, this line refresh starts off with truncating the current buffer so it can fit in a single line on the screen. It truncates so the cursor is always on the screen. As well, while it is truncating, it adds highlighting escape codes to the buffer. There is more information on highlighting later.

After the line is truncated and highlighted, the new line is written to standard output. This is done using a buffer so the line can be cleared and written all at once to avoid a flickering affect. 

#### Search
If `ctrl r` is pressed, that starts the reverse-i-search. The search functions similar to edit where it has a loop that processes every character. However, the input from the user is matched with every entry in the `linenoise` history. The current match and the search input buffer are then both displayed similar to 	`refreshLine()` except both the match and the input buffer are truncated and highlighted. Additionally, the matching section of the match string is underlined when displayed. This allows the user to search through history easily and rerun queries. 

#### Highlighting
For highlighting text via `linenoise`, a highlighting callback is used, this callback points to the `highlight()` function in the `embedded_shell.cpp` file. This is the function that processes the truncation and returns the truncated, highlighted string. Currently, the highlighting works by splitting each word into tokens and checking if the tokens match the keywords. If they do, add the keyword highlighting. This current highlighting will be reworked to handle additional syntax highlighting. 

To highlight the text, the query is first tokenized by the `highlightingTokenize()` function. This function first checks to see if query is a shell command. If it is, the query is tokenized by spaces. The shell command is highlighted as a keyword and all other words are highlighted as string constants. If the function is not a shell command,  the query is parsed as a cypher query.

To tokenize the cypher query, the `CypherLexer` from `Cypher.g4` is utalized. The lexer generates cypher tokens which are converted to highlighting tokens by the `convertToken()` function. This function takes the token and checks the type and converts it to the equivalent highlighting token. Currently, comments can be prased as SP tokens by the lexer so this case is handled by the switch statement. For keywords, the token text is compared against a list of keywords autogenerated by the grammar script. A future update can adjust this script to generate a switch statement of keywords to be more efficient.

Once the tokens are generated, the `linenoiseHighlightText()` function iterates over the tokens and adds the correct highlighting to the buffer. The buffer is then returned to the refresh function to be displayed on the screen.

#### History
All queries and commands entered via the shell are stored in an `history.txt` file. When starting the shell, the history is loaded via the `linenoiseHistoryLoad()` function which accepts a file path. It checks every line and adds it to the `linenoise` history every time the file is cypher complete or it is a `:` shell command. Similarly, the `linenoiseHistorySave()` function accepts a filename and writes the content of the history array to the file.

Queries are added to the `linenoise` history array in the function `linenoiseHistoryAdd()`. If in `:singleline` mode, which is currently the default, all white spaces characters, including newlines and returns, are turned into spaces. Since it can be condensing multiple lines into a single line, every four spaces are turned into one space to remove tabs. Then, if the line is not a duplicate to the previous line, the query is added to the history array.

#### Multiline Differences
There are a few cases where `:multiline` mode is fundamentally different than `:singleline` mode. `:multiline` mode adds extra functionality to the shell, allowing moving between lines and editing, instead of being locked to only editting the most recent line.

The first difference is how the queries get displayed. In the `refreshLine()` function, the input is not truncated to fit on the screen (width) as it can display the input over multiple lines. Instead, the new cursor position and row is calculated. If there are more rows of text than there are terminal rows, the rows are truncated similar to `:singleline`, removing from the beginning and the end to fit the cursor position on the screen. 

Once truncation is done, continue markers are added. These are visual symbols added to the front of the line to indicate what line you are currently adding and make the terminal look nicer. Once the continuation markers are added, the highlight callback from before is called. Again, a buffer is used to clear the screen and write all changes at once.

For multiline, better highlighting is implemented due to the whole query being avaiable. Multiline comments and errors are able to be highlighted in this mode. After the query is tokenized, the highlight errors function is called. This function iterates over every character and checks for comments, unclosed strings, and unclosed brackets. If any errors are found, the highlighting is changed to indicate an error. Highlighting then continues as normal.

For history, the newlines and comments do not need to be ignored as the shell is able to display those characters. Currently, history search is still only `:singleline` mode so when `ctrl_r` is pressed, newlines and returns are converted into spaces. However, when the search is selected, these spaces return back into newlines and are properly displayed by `refreshLine()`.

