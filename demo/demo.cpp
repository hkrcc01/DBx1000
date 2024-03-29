#include <stdio.h>

int main() {
    __uint32_t data = 10;
    void * tp = &data;
    char * p = (char *)tp;
    printf("%02x %02x %02x %02x\r\n", p[0], p[1], p[2], p[3]);
    return 0;
}