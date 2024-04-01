#include "tpcc_helper.h"

drand48_data ** tpcc_buffer;

uint64_t distKey(uint64_t d_id, uint64_t d_w_id)  {
	return d_w_id * DIST_PER_WARE + d_id; 
}

uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
	return (distKey(c_d_id, c_w_id) * g_cust_per_dist + c_id);
}

uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
	return distKey(d_id, w_id) * g_cust_per_dist + o_id; 
}

uint64_t orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
	return orderlineKey(w_id, d_id, o_id); 
}

uint64_t custNPKey(char * c_last, uint64_t c_d_id, uint64_t c_w_id) {
	uint64_t key = 0;
	char offset = 'A';
	for (uint32_t i = 0; i < strlen(c_last); i++) 
		key = (key << 2) + (c_last[i] - offset);
	key = key << 3;
	key += c_w_id * DIST_PER_WARE + c_d_id;
	return key;
}

uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id) {
	return s_w_id * g_max_items + s_i_id;
}

uint64_t Lastname(uint64_t num, char* name) {
  	static const char *n[] =
    	{"BAR", "OUGHT", "ABLE", "PRI", "PRES",
     	"ESE", "ANTI", "CALLY", "ATION", "EING"};
  	strcpy(name, n[num/100]);
  	strcat(name, n[(num/10)%10]);
  	strcat(name, n[num%10]);
  	return strlen(name);
}

uint64_t RAND(uint64_t max, uint64_t thd_id) {
	int64_t rint64 = 0;
	lrand48_r(tpcc_buffer[thd_id], &rint64);
	return rint64 % max;
}

uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id) {
    return x + RAND(y - x + 1, thd_id);
}

uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id) {
  static bool C_255_init = false;
  static bool C_1023_init = false;
  static bool C_8191_init = false;
  static uint64_t C_255, C_1023, C_8191;
  int C = 0;
  switch(A) {
    case 255:
      if(!C_255_init) {
        C_255 = (uint64_t) URand(0,255, thd_id);
        C_255_init = true;
      }
      C = C_255;
      break;
    case 1023:
      if(!C_1023_init) {
        C_1023 = (uint64_t) URand(0,1023, thd_id);
        C_1023_init = true;
      }
      C = C_1023;
      break;
    case 8191:
      if(!C_8191_init) {
        C_8191 = (uint64_t) URand(0,8191, thd_id);
        C_8191_init = true;
      }
      C = C_8191;
      break;
    default:
      M_ASSERT(false, "Error! NURand\n");
      exit(-1);
  }
  return(((URand(0,A, thd_id) | URand(x,y, thd_id))+C)%(y-x+1))+x;
}

uint64_t MakeAlphaString(int min, int max, char* str, uint64_t thd_id) {
    char char_list[] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                        'd','e','f','g','h','i','j','k','l','m','n','o',
                        'p','q','r','s','t','u','v','w','x','y','z','A',
                        'B','C','D','E','F','G','H','I','J','K','L','M',
                        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
    uint64_t cnt = URand(min, max, thd_id);
    for (uint32_t i = 0; i < cnt; i++) 
		str[i] = char_list[URand(0L, 60L, thd_id)];
    for (int i = cnt; i < max; i++)
		str[i] = '\0';

    return cnt;
}

uint64_t MakeNumberString(int min, int max, char* str, uint64_t thd_id) {

  uint64_t cnt = URand(min, max, thd_id);
  for (UInt32 i = 0; i < cnt; i++) {
    uint64_t r = URand(0L,9L, thd_id);
    str[i] = '0' + r;
  }
  return cnt;
}

u_int32_t *** GetAllocArr(char * table_name, char * file_path,  u_int32_t &bank_cnt, u_int32_t &col, char type) {

  u_int32_t row;
  u_int32_t eles_cnt = 0;
  
  FILE *file;
  char line[5000];
  u_int32_t data[5000];

  file = fopen(file_path, "r");
  if (file == NULL) {
    assert(false);
  }

  while (fgets(line, sizeof(line), file)) {

    u_int32_t index = 0;
    char *token = strtok(line, " ");

    while (table_name[index] != '\0' && token[index] != '\0') {
      if (table_name[index] != token[index]) break;
      index++;
    }

    // this table is not matched
    if (table_name[index] != '\0' || token[index] != '\0')
      continue;
    token = strtok(NULL, " ");

    if (type != token[0]) 
      continue;
    token = strtok(NULL, " ");

    sscanf(token, "%d", &bank_cnt);
    token = strtok(NULL, " ");

    sscanf(token, "%d", &row);
    token = strtok(NULL, " ");

    sscanf(token, "%d", &col);
    token = strtok(NULL, " ");

    while (token != NULL) {
      sscanf(token, "%d", &data[eles_cnt++]);  // 将字符串转换为整数
      token = strtok(NULL, " ");
    }
    break;
  }

  eles_cnt = 0;
  u_int32_t ***dynamic = new u_int32_t **[bank_cnt];

	for (u_int32_t i = 0; i < bank_cnt; ++i) {
    dynamic[i] = new u_int32_t *[row];
    for (u_int32_t j = 0; j < row; ++j) {
      dynamic[i][j] = new u_int32_t[col];
      for (u_int32_t k = 0; k < col; ++k) {
        dynamic[i][j][k] = data[eles_cnt++];  // 复制数据
      }
    }
  }

#if TABLE_DATA_DEBUG

  printf("<====");
  for (int index = 0; table_name[index] != '\0'; index++) {
    printf("%c", table_name[index]);
  }
  if (type == '1') {
    printf("_INDEX");
  } else {
    printf("_WIDTH");
  }
  printf("====>\r\n");

  for (u_int32_t i = 0; i < bank_cnt; ++i) {
    printf("{");
    for (u_int32_t j = 0; j < row; ++j) {
      printf("{");
      for (u_int32_t k = 0; k < col; ++k) {
        printf("%d,", dynamic[i][j][k]);
      }
      printf("},");
    }
    printf("},");
  }
  printf("\r\n");
#endif

  return dynamic;
}

uint64_t wh_to_part(uint64_t wid) {
	assert(g_part_cnt <= g_num_wh);
	return wid % g_part_cnt;
}
