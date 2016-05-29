#ifndef MY_ERROR
#define MY_ERROR() do {                                         \
                fprintf(stderr, "\a\nERROR! %s\n file: %s func: %s line: %d\n\n", \
                        strerror(errno), __FILE__, __FUNCTION__, __LINE__); exit(1); \
        } while(0)
#endif

