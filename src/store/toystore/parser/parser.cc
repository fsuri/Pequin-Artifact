// Parser for the custom engine
// Must do syntax checking and generate a query tree.
extern "C"
{
#include "pg_query.h"
}

int main(int argc, char *argv[])
{
    auto ctx = pg_query_parse_init();
    auto result = pg_query_parse("SELECT col1, COUNT(*) from tableA where col2 >= 3 limit 20;");

    if (result.error)
    {
        return 1;
    }

    print_pg_parse_tree(result.tree);

    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    return 0;
}
