#ifndef SEATS_SQL_UPDATE_CUSTOMER_H
#define SEATS_SQL_UPDATE_CUSTOMER_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"

namespace seats_sql {

class SQLUpdateCustomer:public SEATSSQLTransaction {
    public: 
        SQLUpdateCustomer(uint32_t timeout, std::mt19937 &gen);
        virtual ~SQLUpdateCustomer();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t c_id;
        std::string c_id_str;
        int64_t update_ff;
        int64_t attr0; 
        int64_t attr1;
};

struct GetCustomerResultRow {
public:
    GetCustomerResultRow() {}
    ~GetCustomerResultRow() {}
    int64_t c_id;
    std::string c_id_str;
    int64_t c_base_ap_id;
    double c_balance;
    std::string c_sattr00;
    std::string c_sattr01;
    std::string c_sattr02;
    std::string c_sattr03;
    std::string c_sattr04;
    std::string c_sattr05;
    std::string c_sattr06;
    std::string c_sattr07;
    std::string c_sattr08;
    std::string c_sattr09;
    std::string c_sattr10;
    std::string c_sattr11;
    std::string c_sattr12;
    std::string c_sattr13;
    std::string c_sattr14;
    std::string c_sattr15;
    std::string c_sattr16;
    std::string c_sattr17;
    std::string c_sattr18;
    std::string c_sattr19;
    int64_t c_iattr00;
    int64_t c_iattr01;
    int64_t c_iattr02;
    int64_t c_iattr03;
    int64_t c_iattr04;
    int64_t c_iattr05;
    int64_t c_iattr06;
    int64_t c_iattr07;
    int64_t c_iattr08;
    int64_t c_iattr09;
    int64_t c_iattr10;
    int64_t c_iattr11;
    int64_t c_iattr12;
    int64_t c_iattr13;
    int64_t c_iattr14;
    int64_t c_iattr15;
    int64_t c_iattr16;
    int64_t c_iattr17;
    int64_t c_iattr18;
    int64_t c_iattr19;
};

void inline load_row(GetCustomerResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.c_id);
    row->get(1, &store.c_id_str);
    row->get(2, &store.c_base_ap_id);
    row->get(3, &store.c_balance);
    row->get(4, &store.c_sattr00);
    row->get(5, &store.c_sattr01);
    row->get(6, &store.c_sattr02);
    row->get(7, &store.c_sattr03);
    row->get(8, &store.c_sattr04);
    row->get(9, &store.c_sattr05);
    row->get(10, &store.c_sattr06);
    row->get(11, &store.c_sattr07);
    row->get(12, &store.c_sattr08);
    row->get(13, &store.c_sattr09);
    row->get(14, &store.c_sattr10);
    row->get(15, &store.c_sattr11);
    row->get(16, &store.c_sattr12);
    row->get(17, &store.c_sattr13);
    row->get(18, &store.c_sattr14);
    row->get(19, &store.c_sattr15);
    row->get(20, &store.c_sattr16);
    row->get(21, &store.c_sattr17);
    row->get(22, &store.c_sattr18);
    row->get(23, &store.c_sattr19);
    row->get(24, &store.c_iattr00);
    row->get(25, &store.c_iattr01);
    row->get(26, &store.c_iattr02);
    row->get(27, &store.c_iattr03);
    row->get(28, &store.c_iattr04);
    row->get(29, &store.c_iattr05);
    row->get(30, &store.c_iattr06);
    row->get(31, &store.c_iattr07);
    row->get(32, &store.c_iattr08);
    row->get(33, &store.c_iattr09);
    row->get(34, &store.c_iattr10);
    row->get(35, &store.c_iattr11);
    row->get(36, &store.c_iattr12);
    row->get(37, &store.c_iattr13);
    row->get(38, &store.c_iattr14);
    row->get(39, &store.c_iattr15);
    row->get(40, &store.c_iattr16);
    row->get(41, &store.c_iattr17);
    row->get(42, &store.c_iattr18);
    row->get(43, &store.c_iattr19);
}

struct GetFrequentFlyersResultRow {
    GetFrequentFlyersResultRow() {}
    ~GetFrequentFlyersResultRow() {}
    int64_t ff_c_id;
    int64_t ff_al_id;
    std::string ff_c_id_str;
    std::string ff_sattr00;
    std::string ff_sattr01;
    std::string ff_sattr02;
    std::string ff_sattr03;
    int64_t ff_iattr00;
    int64_t ff_iattr01;
    int64_t ff_iattr02;
    int64_t ff_iattr03;
    int64_t ff_iattr04;
    int64_t ff_iattr05;
    int64_t ff_iattr06;
    int64_t ff_iattr07;
    int64_t ff_iattr08;
    int64_t ff_iattr09;
    int64_t ff_iattr10;
    int64_t ff_iattr11;
    int64_t ff_iattr12;
    int64_t ff_iattr13;
    int64_t ff_iattr14;
    int64_t ff_iattr15;
};

void inline load_row(GetFrequentFlyersResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.ff_c_id);
    row->get(1, &store.ff_al_id);
    row->get(2, &store.ff_c_id_str);
    row->get(3, &store.ff_sattr00);
    row->get(4, &store.ff_sattr01);
    row->get(5, &store.ff_sattr02);
    row->get(6, &store.ff_sattr03);
    row->get(7, &store.ff_iattr00);
    row->get(8, &store.ff_iattr01);
    row->get(9, &store.ff_iattr02);
    row->get(10, &store.ff_iattr03);
    row->get(11, &store.ff_iattr04);
    row->get(12, &store.ff_iattr05);
    row->get(13, &store.ff_iattr06);
    row->get(14, &store.ff_iattr07);
    row->get(15, &store.ff_iattr08);
    row->get(16, &store.ff_iattr09);
    row->get(17, &store.ff_iattr10);
    row->get(18, &store.ff_iattr11);
    row->get(19, &store.ff_iattr12);
    row->get(20, &store.ff_iattr13);
    row->get(21, &store.ff_iattr14);
    row->get(22, &store.ff_iattr15);
}

}

#endif

