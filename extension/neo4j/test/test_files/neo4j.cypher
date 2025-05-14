// sudo docker run -it --rm --name neo4j --publish=7474:7474 --publish=7687:7687 --user="$(id -u):$(id -g)" -v /tmp:/tmp -v ~/neo4j/logs:/logs -v ~/neo4j/config:/conf -v ~/neo4j/plugins:/plugins --env NEO4J_AUTH=neo4j/czy990424 --env NEO4J_dbms_usage__report_enabled=false --env NEO4J_server_memory_pagecache_size=4G --env NEO4J_server_memory_heap_max__size=4G -e NEO4J_dbms_security_procedures_unrestricted=gds.\* -e NEO4J_apoc_export_file_enabled=true -e NEO4J_apoc_import_file_use__neo4j__config=false -e JAVA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED neo4j:latest
MATCH ()-[r]->() delete r; MATCH (n) delete n;

// DATA A
CREATE
    (a:Student {type: 1, active: true, age: 35, dob: date("1900-01-01"), height: 5.0, name: "Alice", scores: [96,54,86,92], ratio: 1.731, rank: 0, graduated: false, duration: "3 years 2 days 13 hours 2 minutes", timestamp: datetime("2011-08-20T11:25:30"), uuid: "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", nicknames: ["Aida"], tags: [10,5]}),
    (b:Student {type: 2, active: true, age: 30, dob: date("1900-01-01"), height: 5.1, name: "Bob", scores: [98,42,93,88], ratio: 0.99, rank: 2, graduated: false, duration: "10 years 5 months 13 hours 24 us", timestamp: datetime("2008-11-03T15:25:30.000526"), uuid: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12", nicknames: ["Bobby"], tags: [12,8]}),
    (c:Student {type: 1, active: false, age: 45, dob: date("1940-06-22"), height: 5.0, name: "Carol", scores: [91,75,21,95], ratio: 1.0, rank: 3, graduated: true, duration: "48 hours 24 minutes 11 seconds", timestamp: datetime("1911-08-20T02:32:21"), uuid: "a0eebc999c0b4ef8bb6d6bb9bd380a13", nicknames: ["Carmen","Fred"], tags: [4,5]}),
    (d:Student {type: 2, active: false, age: 20, dob: date("1950-07-23"), height: 4.8, name: "Dan", scores: [76,88,99,89], ratio: 1.3, rank: 5, graduated: true, duration: "10 years 5 months 13 hours 24 us", timestamp: datetime("2031-11-30T12:25:30"), uuid: "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a14", nicknames: ["Wolfeschlegelstein","Daniel"], tags: [1,9]}),
    (e:Student {type: 1, active: false, age: 20, dob: date("1980-10-26"), height: 4.7, name: "Elizabeth", scores: [96,59,65,88], ratio: 1.463, rank: 7, graduated: true, duration: "48 hours 24 minutes 11 seconds", timestamp: datetime("1976-12-23T11:21:42"), uuid: "a0eebc99-9c0b4ef8-bb6d6bb9-bd380a15", nicknames: ["Ein"], tags: [2]}),
    (f:Teacher {code: "57"}),
    (g:Teacher {code: "unknown"}),
    (a)-[:KNOWS {length: 50}]->(b),
    (a)-[:KNOWS {length: 50}]->(c),
    (a)-[:KNOWS {length: 50}]->(d),
    (b)-[:KNOWS {length: 41}]->(a),
    (c)-[:KNOWS {length: 56}]->(a),
    (d)-[:KNOWS {length: 79}]->(a),
    (f)-[:KNOWS {reason: "teach"}]->(a),
    (g)-[:KNOWS {reason: "teach"}]->(a),
    ()-[:RELTOEMPTY2]->(),
    (:Node)-[:RELTOEMPTY1]->(),
    (:AllTypes {
        boolean: true,
        date: date('2023-10-23'),
        duration: duration('P1Y2M3DT4H5M6S'),
        float: 3.1415,
        integer: 42,
        list: ["A", "B", "C"],
        local_datetime: localdatetime('2023-10-23T14:30:45'),
        local_time: localtime('14:30:45'),
        point: point({x: 12.34, y: 56.78}),
        string: 'Sample text',
        zoned_datetime: datetime('2023-10-23T14:30:45+01:00'),
        zoned_time: time('14:30:45+01:00')
    }),
    (:TwoLabels1:TwoLabels2)-[:KNOWS2]->()
;

// DATA B
CREATE (u:User {name: 'Adam', age: 30});
CREATE (u:User {name: 'Karissa', age: 40});
CREATE (u:User {name: 'Zhang', age: 50});
CREATE (u:User {name: 'Noura', age: 25});
CREATE (c:City {name: 'Waterloo', population: 150000});
CREATE (c:City {name: 'Kitchener', population: 200000});
CREATE (c:City {name: 'Guelph', population: 75000});
CREATE (a)-[:RANDOMREL]->(b);
CREATE (a:badLabel)-[:RANDOMREL]->(b);
CREATE (a)-[:RANDOMREL]->(b:greatLabel);
CREATE (a:okLabel)-[:RANDOMREL]->(b:okLabel);
MATCH (u:User {name: 'Adam'}), (c:City {name: 'Waterloo'}) CREATE (u)-[:LivesIn]->(c);
MATCH (u:User {name: 'Karissa'}), (c:City {name: 'Waterloo'}) CREATE (u)-[:LivesIn]->(c);
MATCH (u:User {name: 'Zhang'}), (c:City {name: 'Kitchener'}) CREATE (u)-[:LivesIn]->(c);
MATCH (u:User {name: 'Noura'}), (c:City {name: 'Guelph'}) CREATE (u)-[:LivesIn]->(c);
MATCH (u:User {name: 'Adam'}), (u1:User {name: 'Karissa'}) CREATE (u)-[:Follows {since: 2020}]->(u1);
MATCH (u:User {name: 'Adam'}), (u1:User {name: 'Zhang'}) CREATE (u)-[:Follows {since: 2020}]->(u1);
MATCH (u:User {name: 'Karissa'}), (u1:User {name: 'Zhang'}) CREATE (u)-[:Follows {since: 2021}]->(u1);
MATCH (u:User {name: 'Zhang'}), (u1:User {name: 'Noura'}) CREATE (u)-[:Follows {since: 2022}]->(u1);
