#include <pthread.h>
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#define Size 4

/*typedef struct arg
{
    int nr_Mapper, nr_Reducer;
    char *fisier;
}Targ;
*/

void get_args(int argc, char *argv[], int *nr_Mapper, int *nr_Reducer, char *fisier)
{
    if(argc < 3)
    {
        printf("Numar insuficient de parametri\n");
        exit(1);
    }
    *nr_Mapper = atoi(argv[1]);
    *nr_Reducer = atoi(argv[2]);
    fisier = strdup(argv[3]);
}

int main(int argc, char *argv[])
{
    int nr_Mapper, nr_Reducer;
    char *fisier = NULL;
    get_args(argc,argv,nr_Mapper, nr_Reducer, fisier);
    printf("%d %d \n %s\n",nr_Mapper, nr_Reducer, fisier);

}