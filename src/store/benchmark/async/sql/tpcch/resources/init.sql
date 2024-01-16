DROP TABLE IF EXISTS history CASCADE;
DROP TABLE IF EXISTS new_order CASCADE;
DROP TABLE IF EXISTS order_line CASCADE;
DROP TABLE IF EXISTS "order" CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS district CASCADE;
DROP TABLE IF EXISTS stock CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS warehouse CASCADE;

CREATE TABLE warehouse (
    id       int         NOT NULL,
    name     TEXT        NOT NULL,
    street_1 TEXT        NOT NULL,
    street_2 TEXT        NOT NULL,
    city     TEXT        NOT NULL,
    state    TEXT        NOT NULL,
    zip      TEXT        NOT NULL,
    tax      INT         NOT NULL, 
    ytd      INT         NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE item (
    id    int           NOT NULL,
    im_id int           NOT NULL,
    name  text   NOT NULL,
    price INT           NOT NULL,
    data  text   NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE stock (
    i_id       int           NOT NULL,
    w_id       int           NOT NULL,
    quantity   int           NOT NULL,
    dist_01    text          NOT NULL,
    dist_02    text          NOT NULL,
    dist_03    text          NOT NULL,
    dist_04    text          NOT NULL,
    dist_05    text          NOT NULL,
    dist_06    text          NOT NULL,
    dist_07    text          NOT NULL,
    dist_08    text          NOT NULL,
    dist_09    text          NOT NULL,
    dist_10    text          NOT NULL,
    ytd        int           NOT NULL,
    order_cnt  int           NOT NULL,
    remote_cnt int           NOT NULL,
    data       text          NOT NULL,
    FOREIGN KEY (w_id) REFERENCES warehouse (id) ON DELETE CASCADE,
    FOREIGN KEY (i_id) REFERENCES item (id) ON DELETE CASCADE,
    PRIMARY KEY (w_id, i_id)
);

CREATE TABLE district (
    id          int            NOT NULL,
    w_id        int            NOT NULL,
    name      text           NOT NULL,
    street_1  text            NOT NULL,
    street_2  text            NOT NULL,
    city      text            NOT NULL,
    state     text            NOT NULL,
    zip       text            NOT NULL,
    ytd       int             NOT NULL,
    tax       int             NOT NULL,
    next_o_id int            NOT NULL,
    FOREIGN KEY (w_id) REFERENCES warehouse (id) ON DELETE CASCADE,
    PRIMARY KEY (w_id, id)
);

CREATE TABLE customer (
    id         int            NOT NULL,
    d_id         int            NOT NULL,
    w_id           int            NOT NULL,
    first           text          NOT NULL, 
    middle          text          NOT NULL, 
    last            text          NOT NULL,
    street_1     text       NOT NULL,
    street_2     text       NOT NULL,
    city         text       NOT NULL,
    state        text          NOT NULL,
    zip          text        NOT NULL,
    phone        text             NOT NULL,
    since        int              NOT NULL,
    credit       text        NOT NULL,
    credit_lim   int NOT NULL,

    discount     int   NOT NULL,
    balance      int  NOT NULL,
    ytd_payment  int          NOT NULL,
    payment_cnt  int            NOT NULL,
    delivery_cnt int            NOT NULL,
    data         text   NOT NULL,
    FOREIGN KEY (w_id, d_id) REFERENCES district (w_id, id) ON DELETE CASCADE,
    PRIMARY KEY (w_id, d_id, id)
);

CREATE TABLE history (
    c_id   int           NOT NULL,
    c_d_id int           NOT NULL,
    c_w_id int           NOT NULL,
    d_id   int           NOT NULL,
    w_id   int           NOT NULL,
    date   int          NOT NULL,
    amount int NOT NULL,
    data   text   NOT NULL,
    FOREIGN KEY (w_id, d_id, c_id) REFERENCES customer (w_id, d_id, id) ON DELETE CASCADE,
    FOREIGN KEY (w_id, d_id) REFERENCES district (w_id, id) ON DELETE CASCADE
);

CREATE TABLE "order" (
    id       int       NOT NULL,
    d_id       int       NOT NULL,
    w_id         int       NOT NULL,
    c_id       int       NOT NULL,
    entry_d    int NOT NULL,
    carrier_id int                DEFAULT NULL,
    ol_cnt     int       NOT NULL,
    all_local  boolean       NOT NULL,
    PRIMARY KEY (w_id, d_id, id),
    FOREIGN KEY (w_id, d_id, c_id) REFERENCES customer (w_id, d_id, id) ON DELETE CASCADE,
    UNIQUE (w_id, d_id, c_id, id)
);

CREATE TABLE new_order (
    o_id int NOT NULL,
    d_id int NOT NULL,
    w_id int NOT NULL,
    FOREIGN KEY (w_id, d_id, o_id) REFERENCES "order" (w_id, d_id, id) ON DELETE CASCADE,
    PRIMARY KEY (w_id, d_id, o_id)
);

CREATE TABLE order_line (
    o_id        int           NOT NULL,
    d_id        int           NOT NULL,
    w_id        int           NOT NULL,
    number      int           NOT NULL,
    i_id        int           NOT NULL,
    supply_w_id int           NOT NULL,
    delivery_d  int           ,
    quantity    int  NOT NULL,

    amount      int NOT NULL,
    dist_info   text      NOT NULL,
    FOREIGN KEY (w_id, d_id, o_id) REFERENCES "order" (w_id, d_id, id) ON DELETE CASCADE,
    FOREIGN KEY (supply_w_id, i_id) REFERENCES stock (w_id, i_id) ON DELETE CASCADE,
    PRIMARY KEY (w_id, d_id, o_id, number)
);

CREATE INDEX idx_customer_name ON customer (w_id, d_id, last, first);

CREATE TABLE region (
    r_regionkey int       NOT NULL,
    r_name      text  NOT NULL,
    r_comment   text NOT NULL,
    PRIMARY KEY (r_regionkey)
);

CREATE TABLE nation (
    n_nationkey int       NOT NULL,
    n_name      text  NOT NULL,
    n_regionkey int       NOT NULL,
    n_comment   text NOT NULL,
    FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey) ON DELETE CASCADE,
    PRIMARY KEY (n_nationkey)
);
CREATE INDEX n_rk ON nation (n_regionkey ASC);

CREATE TABLE supplier (
    su_suppkey   int            NOT NULL,
    su_name      text       NOT NULL,
    su_address   text    NOT NULL,
    su_nationkey int            NOT NULL,
    su_phone     text       NOT NULL,
    su_acctbal   float             NOT NULL,
    su_comment   text      NOT NULL,
    FOREIGN KEY (su_nationkey) REFERENCES nation (n_nationkey) ON DELETE CASCADE,
    PRIMARY KEY (su_suppkey)
);
CREATE INDEX s_nk ON supplier (su_nationkey ASC);