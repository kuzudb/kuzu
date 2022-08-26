#include "include/main_test_helper.h"

TEST_F(ApiTest, CatalogPrinting) {
    ASSERT_STREQ(conn->getNodeTableNames().c_str(), "Node tables: \n\tperson\n\torganisation\n");
    ASSERT_STREQ(
        conn->getRelTableNames().c_str(), "Rel tables: \n\tknows\n\tstudyAt\n\tworkAt\n\tmeets\n");

    ASSERT_STREQ(conn->getRelPropertyNames("studyAt").c_str(),
        "studyAt src nodes: \n\tperson\nstudyAt dst nodes: \n\torganisation\nstudyAt properties: "
        "\n\tyear INT64\n\tplaces STRING[]\n\t_id INT64\n");
    try {
        conn->getNodePropertyNames("dummy");
        FAIL();
    } catch (Exception& exception) {
        ASSERT_STREQ(exception.what(), "Cannot find node table dummy");
    }
}
