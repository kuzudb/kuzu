#include <signal.h>
#include <sys/mman.h>
#include <unistd.h>

#include "main_test_helper/main_test_helper.h"

using namespace kuzu::testing;
using namespace kuzu::common;
using namespace kuzu::main;

class DBLockingTest : public ApiTest {
    void SetUp() override { BaseGraphTest::SetUp(); }
};

TEST_F(DBLockingTest, testReadLock) {
    uint64_t* count = (uint64_t*)mmap(
        NULL, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
    *count = 0;
    // create db
    EXPECT_NO_THROW(createDBAndConn());
    ASSERT_TRUE(conn->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (:Person {name: 'Alice', age: 25});")->isSuccess());
    database.reset();
    // test read write db
    pid_t pid = fork();
    if (pid == 0) {
        systemConfig->readOnly = true;
        EXPECT_NO_THROW(createDBAndConn());
        (*count)++;
        ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
        while (true) {
            usleep(100);
            ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
        }
    } else if (pid > 0) {
        while (*count == 0) {
            usleep(100);
        }
        systemConfig->readOnly = false;
        // try to open db for writing, this should fail
        EXPECT_ANY_THROW(createDBAndConn());
        // but opening db for reading should work
        systemConfig->readOnly = true;
        EXPECT_NO_THROW(createDBAndConn());
        ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
        // kill the child
        if (kill(pid, SIGKILL) != 0) {
            FAIL();
        }
    }
}

TEST_F(DBLockingTest, testWriteLock) {
    uint64_t* count = (uint64_t*)mmap(
        NULL, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
    *count = 0;
    // test write lock
    // fork away a child
    pid_t pid = fork();
    if (pid == 0) {
        // child process
        // open db for writing
        systemConfig->readOnly = false;
        EXPECT_NO_THROW(createDBAndConn());
        // opened db for writing
        // insert some values
        (*count)++;
        ASSERT_TRUE(
            conn->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
                ->isSuccess());
        ASSERT_TRUE(conn->query("CREATE (:Person {name: 'Alice', age: 25});")->isSuccess());
        while (true) {
            ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
            usleep(100);
        }
    } else if (pid > 0) {
        // parent process
        // sleep a bit to wait for child process
        while (*count == 0) {
            usleep(100);
        }
        // try to open db for writing, this should fail
        systemConfig->readOnly = false;
        EXPECT_ANY_THROW(createDBAndConn());
        // try to open db for reading, this should fail
        systemConfig->readOnly = true;
        EXPECT_ANY_THROW(createDBAndConn());
        // kill the child
        if (kill(pid, SIGKILL) != 0) {
            FAIL();
        }
    }
}

TEST_F(DBLockingTest, testReadOnly) {
    uint64_t* count = (uint64_t*)mmap(
        NULL, sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, 0, 0);
    *count = 0;
    // cannot create a read-only database in a new directory
    systemConfig->readOnly = true;
    EXPECT_ANY_THROW(createDBAndConn());

    // create the database file and initialize it with data
    pid_t create_pid = fork();
    if (create_pid == 0) {
        systemConfig->readOnly = false;
        EXPECT_NO_THROW(createDBAndConn());
        ASSERT_TRUE(
            conn->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));")
                ->isSuccess());
        ASSERT_TRUE(conn->query("CREATE (:Person {name: 'Alice', age: 25});")->isSuccess());
        exit(0);
    }
    waitpid(create_pid, NULL, 0);

    // now connect in read-only mode
    systemConfig->readOnly = true;
    EXPECT_NO_THROW(createDBAndConn());
    // we can query the database
    ASSERT_TRUE(conn->query("MATCH (:Person) RETURN COUNT(*)")->isSuccess());
    // however, we can't perform DDL statements
    EXPECT_ANY_THROW(conn->query("CREATE NODE TABLE university(ID INT64, PRIMARY KEY(ID))"));
    EXPECT_ANY_THROW(conn->query("ALTER TABLE Peron DROP name"));
    EXPECT_ANY_THROW(conn->query("DROP TABLE Peron"));
    // neither can we insert/update/delete data
    EXPECT_ANY_THROW(conn->query("CREATE (:Person {name: 'Bob', age: 25});"));
    EXPECT_ANY_THROW(conn->query("MATCH (p:Person) WHERE p.name='Alice' SET p.age=26;"));
    EXPECT_ANY_THROW(conn->query("MATCH (p:Person) WHERE name='Alice' DELETE p;"));
}
