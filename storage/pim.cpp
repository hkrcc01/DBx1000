#include "pim.h"

#if PIM_ENABLE

// <================class field================>


// <================class part================>

void part::init(u_int32_t width) {
    this->width = width;
    u_int32_t banks_cnt = BANK_CNT_PER_RANK * RANK_CNT;
    this->head = new void*[banks_cnt];

#if DETLA_STORAGE_ENABLE
    this->detla = new void*[banks_cnt];
#endif
}

// <================class table================>

void table_s::insert_tuple(void ** data, u_int32_t &index) {
    field * f_tmp;
    u_int32_t part_no;
    u_int32_t row_no;
    u_int32_t col_no;
    u_int32_t size;
    u_int32_t banks_cnt = BANK_CNT_PER_RANK * RANK_CNT;
    u_int32_t devices_cnt = DEVICE_CNT_PER_BANK;

    char * data_tmp;
    char * tar_tmp;

    for (u_int32_t f = 0; f < fields_cnt; f++) {
        f_tmp = this->fields[f];
        data_tmp = (char*)data[f];
		for (u_int32_t fc = 0; fc < f_tmp->cnt; fc++) {
            part_no = f_tmp->banks_id[fc];
            row_no = f_tmp->devices_id[fc];
            col_no = f_tmp->indexs[fc];
            size = f_tmp->sizes[fc];

            // tuple_cnt decribles the number of tuples now
            // get the bank id
            tar_tmp = (char*)parts[part_no]->head[(tuple_cnt) % banks_cnt];
            // get the row id
            tar_tmp += (tuple_cnt / banks_cnt) * parts[part_no]->width * devices_cnt;
            // get the device id
            tar_tmp += row_no * parts[part_no]->width;
            // get the offset of this device 
            tar_tmp += col_no;
            memcpy(tar_tmp, data_tmp, size);
            // update data pointer
            data_tmp += size;
		}
    }
    index = tuple_cnt++;
}

void table_s::get_value(u_int32_t field_index, u_int32_t row_index, void * &value) {
    u_int32_t part_no;
    u_int32_t row_no;
    u_int32_t col_no;
    u_int32_t size;
    u_int32_t banks_cnt = BANK_CNT_PER_RANK * RANK_CNT;
    u_int32_t devices_cnt = DEVICE_CNT_PER_BANK;

    field * f_tmp = this->fields[field_index];
    char * tar_tmp;
    char * t_value = (char*)value;

    for (u_int32_t fc = 0; fc < f_tmp->cnt; fc++) {
        part_no = f_tmp->banks_id[fc];
        row_no = f_tmp->devices_id[fc];
        col_no = f_tmp->indexs[fc];
        size = f_tmp->sizes[fc];

        // tuple_cnt decribles the number of tuples now
        // get the bank id
        tar_tmp = (char*)parts[part_no]->head[(row_index) % banks_cnt];
        // get the row id
        tar_tmp += (row_index / banks_cnt) * parts[part_no]->width * devices_cnt;
        // get the device id
        tar_tmp += row_no * parts[part_no]->width;
        // get the offset of this device 
        tar_tmp += col_no;
        memcpy(t_value, tar_tmp, size);
        // update data pointer
        t_value += size;
    }

}

#if DETLA_STORAGE_ENABLE

void table_s::detla_update_and_invalid(u_int32_t version_id, u_int32_t storage_index) {
    char * data_tmp;
    char * detla_tmp;

    u_int32_t banks_cnt = BANK_CNT_PER_RANK * RANK_CNT;
    u_int32_t devices_cnt = DEVICE_CNT_PER_BANK;
    u_int32_t version_num = VERSION_NUM;

    for (u_int32_t p = 0; p < parts_cnt; p++) {
        data_tmp = (char*)parts[p]->head[(storage_index) % banks_cnt];
        detla_tmp = (char*)parts[p]->detla[((storage_index ) % banks_cnt)];

        data_tmp += (storage_index / banks_cnt) * parts[p]->width * devices_cnt;
        detla_tmp += (storage_index / banks_cnt) * version_num * parts[p]->width * devices_cnt;
        detla_tmp += version_id * parts[p]->width * devices_cnt;

        memcpy(data_tmp, detla_tmp, parts[p]->width * devices_cnt);
    }
}

void table_s::insert_detla(u_int32_t storage_index, u_int32_t version_id, row_t * r) {
    field * f_tmp;
    u_int32_t part_no;
    u_int32_t row_no;
    u_int32_t col_no;
    u_int32_t size;
    u_int32_t banks_cnt = BANK_CNT_PER_RANK * RANK_CNT;
    u_int32_t devices_cnt = DEVICE_CNT_PER_BANK;
    u_int32_t version_num = VERSION_NUM;

    char * data_tmp;
    char * tar_tmp;

    for (u_int32_t f = 0; f < fields_cnt; f++) {
        f_tmp = this->fields[f];
        data_tmp = (char*) r->get_value(f);
		for (u_int32_t fc = 0; fc < f_tmp->cnt; fc++) {
            part_no = f_tmp->banks_id[fc];
            row_no = f_tmp->devices_id[fc];
            col_no = f_tmp->indexs[fc];
            size = f_tmp->sizes[fc];

            // tuple_cnt decribles the number of tuples now
            // get the bank id
            tar_tmp = (char*)parts[part_no]->detla[(storage_index) % banks_cnt];
            // get the row id
            tar_tmp += (storage_index / banks_cnt) * version_num * parts[part_no]->width * devices_cnt;
            // get the version id
            tar_tmp += version_id * parts[part_no]->width * devices_cnt;
            // get the device id
            tar_tmp += row_no * parts[part_no]->width;
            // get the offset of this device
            tar_tmp += col_no;
            memcpy(tar_tmp, data_tmp, size);
            // update data pointer
            data_tmp += size;
		}
    }
}

#endif

table_s* table_s::init_table(char * name, u_int32_t name_len, u_int32_t *** mapping_index, u_int32_t *** mapping_width,
                        u_int32_t banks_cnt, u_int32_t row, u_int32_t col, u_int32_t fields_cnt) {
    this->tuple_cnt = 0;
    this->name = (char*)_mm_malloc(name_len, 1);
    memcpy(this->name, name, name_len);
    this->parts_cnt = banks_cnt;
    this->fields_cnt = fields_cnt;

    fields = new field*[fields_cnt];
    for (u_int32_t i = 0; i < fields_cnt; i++) {
        fields[i] = new field();
    }
    parts = new part*[banks_cnt];

    u_int32_t part_width;
    for (u_int32_t bc = 0; bc < banks_cnt; bc++) {
        part_width = 0;
        for (u_int32_t r = 0; r < row; r++) {
            for (u_int32_t c = 0; c < col; c++) {
                if (mapping_index[bc][r][c] == 0)
                    break;

                if (r == 0)
                    part_width += mapping_width[bc][r][c];

                fields[mapping_index[bc][r][c]-1]->cnt++;
            }
        }
        parts[bc] = new part();
        parts[bc]->init(part_width);
    }

    // init all massage of fields
    for (u_int32_t i = 0; i < fields_cnt; i++) {

        u_int32_t cnt = fields[i]->cnt;

        fields[i]->cnt = 0;
        fields[i]->banks_id = (u_int32_t*)_mm_malloc(sizeof(u_int32_t) * cnt, 1);
        fields[i]->devices_id = (u_int32_t*)_mm_malloc(sizeof(u_int32_t) * cnt, 1);
        fields[i]->indexs = (u_int32_t*)_mm_malloc(sizeof(u_int32_t) * cnt, 1);
        fields[i]->sizes = (u_int32_t*)_mm_malloc(sizeof(u_int32_t) * cnt, 1);
    }

    for (u_int32_t bc = 0; bc < banks_cnt; bc++) {
        for (u_int32_t r = 0; r < row; r++) {

            u_int32_t prefix_sum = 0;

            for (u_int32_t c = 0; c < col; c++) {
                if (mapping_index[bc][r][c] == 0) 
                    break;
                u_int32_t cnt = fields[mapping_index[bc][r][c]-1]->cnt;

                fields[mapping_index[bc][r][c]-1]->banks_id[cnt] = bc;
                fields[mapping_index[bc][r][c]-1]->devices_id[cnt] = r;
                fields[mapping_index[bc][r][c]-1]->indexs[cnt] = prefix_sum;
                fields[mapping_index[bc][r][c]-1]->sizes[cnt] = mapping_width[bc][r][c];
                fields[mapping_index[bc][r][c]-1]->cnt++;

                prefix_sum += mapping_width[bc][r][c];
            }
        }
    }
    return this;
}

void table_s::init_table_size(u_int32_t row_cnt) {
    u_int32_t banks_cnt = BANK_CNT_PER_RANK * RANK_CNT;

    for (u_int32_t p = 0; p < parts_cnt; p++) {
        // Rounded up
        u_int32_t row_per_bank = (row_cnt + banks_cnt - 1) / banks_cnt;

        for (u_int32_t b = 0; b < banks_cnt; b++) {
            parts[p]->head[b] = _mm_malloc(DEVICE_CNT_PER_BANK * parts[p]->width * row_per_bank, 1);

#if DETLA_STORAGE_ENABLE
            parts[p]->detla[b] = _mm_malloc(DEVICE_CNT_PER_BANK * parts[p]->width * row_per_bank * VERSION_NUM, 1);
#endif
        }
    }
}

void table_s::print_line(u_int32_t line_index) {
    part * pa;
    unsigned char * bytePtr;
    u_int32_t banks_cnt = BANK_CNT_PER_RANK * RANK_CNT;
    u_int32_t devices_cnt = DEVICE_CNT_PER_BANK;

    for (u_int32_t i = 0; i < parts_cnt; i++) {
        printf("<====DEBUG_MSG_");
        
        for (int nn = 0; name[nn] != '\0'; nn++) {
            printf("%c", name[nn]);
        }
        
        printf("_LINE_%d_PARTATION_%d====>:\r\n", line_index, i);

        pa = parts[i];

        // get the bank id
        bytePtr = (unsigned char *)pa->head[line_index % banks_cnt];
        // get the row id
        bytePtr += (line_index / banks_cnt) * devices_cnt * pa->width;

        for (u_int32_t j = 0; j < devices_cnt; j++) {
            printf("DEVICE_%d----> ", j);
            for (u_int32_t k = 0; k < pa->width; k++){
                printf("%02x ", bytePtr[j * pa->width + k]);
            }
            printf("\r\n");
        }

        printf("<");
        for (int nn = 0; name[nn] != '\0'; nn++) {
            printf("=");
        }
        printf("=====================================>\r\n");
    }
}

// <===============class storage================>
void storage::init(int table_cnt) {
    this->table_cnt = table_cnt;
    this->tmp_cnt = table_cnt;
    tables = new table_s*[table_cnt];
}

table_s* storage::insert_table(char * name, u_int32_t name_len, u_int32_t *** mapping_index, u_int32_t *** mapping_width,
                        u_int32_t banks_cnt, u_int32_t row, u_int32_t col, u_int32_t fields_cnt) {
    tables[--tmp_cnt] = new table_s();
    return tables[tmp_cnt]->init_table(name, name_len, mapping_index, mapping_width, banks_cnt, row, col, fields_cnt);
}

RC storage::get_table(char * name, table_s * &t) {
    for (int i = 0; i < table_cnt; i++) {
        if (strcmp(tables[i]->name , name) == 0){
            t = tables[i];
            return RCOK;
        }
    }
    assert(false);
}

#endif