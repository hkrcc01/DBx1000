#include "global.h"
#include "row.h"

#if PIM_ENABLE

class field {
public: 
    u_int32_t * banks_id;
    u_int32_t * devices_id;
    u_int32_t * indexs;
    u_int32_t * sizes;
    u_int32_t cnt;
};

class part {
public:
    void init(u_int32_t width);

    u_int32_t width;
    void ** head;

#if DETLA_STORAGE_ENABLE
    void ** detla;
#endif

};

class table_s {
public:
    // void init();

    table_s* init_table(char * name, u_int32_t name_len, u_int32_t *** mapping_index, u_int32_t *** mapping_width,
                        u_int32_t banks_cnt, u_int32_t row, u_int32_t col, u_int32_t fields_cnt);

    void init_table_size(u_int32_t row_cnt);

    void insert_tuple(void ** data, u_int32_t &index);
    void detla_update_and_invalid(u_int32_t version_id, u_int32_t storage_index);
    void insert_detla(u_int32_t storage_index, u_int32_t version_id, row_t * r);

    void print_line(u_int32_t line_index);
    void print_detla_line(u_int32_t version_id, u_int32_t storage_index);

    void get_value(u_int32_t field_index, u_int32_t row_index, void * &value);

    char * name;

    field ** fields;
    part ** parts;

    // one table contain some partition and fields
    u_int32_t fields_cnt;
    u_int32_t parts_cnt;

    u_int32_t tuple_cnt;
};

class storage{
public:
    void init(int table_cnt);

    table_s* insert_table(char * name, u_int32_t name_len, u_int32_t *** mapping_index, u_int32_t *** mapping_width,
                        u_int32_t banks_cnt, u_int32_t row, u_int32_t col, u_int32_t fields_cnt);

    RC get_table(char * name, table_s * &t);
    
    // workloads contain a lot of tables
    int table_cnt;
    int tmp_cnt;
    table_s ** tables;
};

#endif