# Server API Endpoints:
 - [Change the bufferPool size](#1-change-the-bufferpool-size)
 - [Get the current bufferPool size](#2-get-the-current-bufferpool-size)
 - [Load a Graph](#3-load-a-graph)
 - [Print the query plan in a human-readable format](#4-print-the-query-plan-in-a-human-readable-format)
 - [Execute a query plan](#5-execute-a-query-plan)
------

### 1. Change the bufferPool size.
* **Method:** 

	`PUT` 
* **URL**:

	/bufferPoolSize
* **DataParams:**
	
	**[REQ]** Intended size of the bufferPool in Bytes. The size should be in multiple of 4096 (system PAGE_SIZE). Current bounds are 1MB and 1TB. 	
	
	`33554432`
* **Success Response:**
	
	- Code: `200 (OK)`
	- Content `{msg : "BufferPool size has been set to 33554432 B"}`
* **Error Response(s):**
	- Code: `400 (Bad Request)`
	- Content `{error : "no Change in size."}`
	<br>
	
	- Code: `400 (Bad Request)`
	- Content `{error : "Buffer pool size should be aligned to the system PAGE SIZE (=4096B)"}`
	<br>	
	
	- Code: `400 (Bad Request)`
	- Content `{error : "Buffer pool size argument should be graeter than 1MB or less than 1TB."}`
* **Note**

	The previously loaded Graph is dropped on successful change of bufferSize.


### 2. Get the current bufferPool size.
* **Method:** 

	`GET` 


* **URL**:

	/bufferPoolSize


* **Success Response:**
	
	- Code: `200 (OK)`
	- Content `{msg : "33554432 B"}`
	
	
### 3. Load a Graph.
* **Method:** 

	`POST` 
* **URL**:

	/load
* **DataParams:**
	
	**[REQ]** Absolute path of the directory containing the serialized graph data.
	
	`/home/foo/bar/...ser/`
* **Success Response:**
	
	- Code: `200 (OK)`
	- Content `{msg : "Intialized the Graph at /home/foo/bar/.../ser/"}`
* **Error Response(s):**
	- Code: `400 (Bad Request)`
	- Content `{error : <system-error-msg>}`


### 4. Print the query plan in a human-readable format.
* **Method:** 

	`GET` 
* **URL**:

	/prettyPrint
* **DataParams**
	**[REQ]** Absolute path of the serialized query plan
	
	`/home/foo/bar/.../test.plan`
* **Success Response:**
	
	TBD
* **Error Response:**
	
	TBD	
	
	
### 5. Execute a query plan
* **Method:** 

	`POST` 
* **URL**:

	/execute
* **DataParams**
	**[REQ]** Absolute path of the directory containing pre-serialized graph data.
	
	`/home/foo/bar/.../test.plan`
* **Success Response:**
	
	TBD
* **Error Response:**
	
	TBD



