#include <pthread.h>
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#define Size 4
#define Len_Title 30

typedef struct arg
{
    char **fisiere;
    int index, nr_fisiere_ramase;
    pthread_mutex_t mutex;
    int **liste_partiale;
}Targ;


int maxim(int a, int b)
{
    if(a >= b)
    {
        return a;
    }
    return b;
}

void get_args(int argc, char *argv[], int *nr_Mapper, int *nr_Reducer, char **fisier)
{
    if(argc < 3)
    {
        printf("Numar insuficient de parametri\n");
        exit(1);
    }
    *nr_Mapper = atoi(argv[1]);
    *nr_Reducer = atoi(argv[2]);
    *fisier = strdup(argv[3]);
}

void *fct(void *arg)
{
    Targ args = *(Targ *) arg;
    while(args.nr_fisiere_ramase > 0)
    {
        pthread_mutex_lock(&args.mutex);
        FILE *f = fopen(args.fisiere[args.index], "r");
        if( f != NULL)
        {
            args.index++;
            args.nr_fisiere_ramase--;
        } 
        pthread_mutex_unlock(&args.mutex);
        int nr_valori;
        char *string = calloc(Len_Title, sizeof(char));
        fgets(string, Len_Title, f);
        nr_valori = atoi(string);

    }

    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    int nr_Mapper, nr_Reducer;
    char *fisier = NULL;
    get_args(argc,argv,&nr_Mapper, &nr_Reducer, &fisier);
    FILE *f0 = fopen(fisier, "r");
    int nr_fisiere;
    char *string_nr_fisiere = calloc(Len_Title, sizeof(char));
    fgets(string_nr_fisiere, Len_Title, f0);
    nr_fisiere = atoi(string_nr_fisiere);
    //printf("%d\n", nr_fisiere);
    char **fisiere = calloc(nr_fisiere, sizeof(char *));
    for(int i = 0; i < nr_fisiere; i++)
    {
        fisiere[i] = calloc(Len_Title, sizeof(char));
        fgets(fisiere[i], Len_Title, f0);
    }

    int nr_threads = maxim(nr_Mapper, nr_Reducer);
    pthread_t *threads = calloc(nr_threads, sizeof(pthread_t));
    Targ args;
    args.fisiere = fisiere;
    args.index = 0;
    args.nr_fisiere_ramase = nr_fisiere;
    pthread_mutex_init(&args.mutex, NULL);
    for(int i = 0; i < nr_threads; i++)
    {
        int r = pthread_create(&threads[i], NULL, fct, (void *)&args);
        if (r)
        {
            printf("Eroare la crearea thread-ului %d\n", i);
            exit(-1);
        }
    }
    for(int i = 0; i < nr_threads; i++)
    {
        int r = pthread_join(threads[i],NULL);
        if(r)
        {
            printf("Eroare la asteptarea thread-ului %d\n", i);
            exit(-1);
        }
    }

    pthread_mutex_destroy(&args.mutex);
    
    pthread_exit(NULL);


}