// Parser for the custom engine
// Must do syntax checking and generate a query tree.
extern "C"
{
#include <pg_query.h>
}

#include <stdio.h>
#include <cassert>
// #include <lib/assert.h>
#include <nlohmann/json.hpp>
using json = nlohmann::json;

int main(int argc, char *argv[])
{
#ifdef ce_integrated // this serves to practice the logging conventions of pequinstore
    Debug("Inside custom parser")
#else
#endif

    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s <sql_query>\n", argv[0]);
        return 1;
    }

    PgQueryParseResult result;
    result = pg_query_parse(argv[1]);
    json j = json::parse(result.parse_tree, nullptr, false);

    printf("%s\n", j.dump(2).c_str());

#ifdef ce_integrated
    UW_ASSERT(j["stmts"].size() == 1); // Only supporting one statement for now
#else
    assert(j["stmts"].size() == 1);
#endif

    pg_query_free_parse_result(result); // for valgrind
}
