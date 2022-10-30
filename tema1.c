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
    int nr_Mapper, nr_Reducer, id_thread,a_luat_fisier;
    pthread_mutex_t *mutex;
    pthread_barrier_t *bariera;
    int **liste_partiale;
    int *size_liste_partiale;
    int *capacity_liste_partiale;
    int *rezultate, *nr_fisiere_ramase, *index ;
    int reduce;
} Targ;

int maxim(int a, int b)
{
    if (a >= b)
    {
        return a;
    }
    return b;
}

void get_args(int argc, char *argv[], int *nr_Mapper, int *nr_Reducer, char **fisier)
{
    if (argc < 3)
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
    Targ *args = (Targ *)arg;
    while (*(args->nr_fisiere_ramase) > 0)
    {
        pthread_mutex_lock(args->mutex);
        if (*(args->nr_fisiere_ramase) > 0)
        {
            FILE *f = fopen(args->fisiere[*(args->index)], "r");
            if (f != NULL)
            {
                printf("Id:%d deschide %s\n ", args->id_thread,args->fisiere[*(args->index)]);
                args->a_luat_fisier = 1;
                *(args->index) = *(args->index) + 1;
                *(args->nr_fisiere_ramase) = *(args->nr_fisiere_ramase) - 1;
            }
            pthread_mutex_unlock(args->mutex);

            int nr_valori;
            char *string = calloc(Len_Title, sizeof(char));
            fgets(string, Len_Title, f);
            nr_valori = atoi(string);
            for (int i = 0; i < args->nr_Reducer; i++)
            {

                int capacity = args->capacity_liste_partiale[i] = 20;
                args->size_liste_partiale[i] = 0;
                args->liste_partiale[i] = calloc(capacity, sizeof(int));
            }

            for (int i = 0; i < nr_valori; i++)
            {
                fgets(string, Len_Title, f);
                int nr = atoi(string);
                for (int putere = 2; putere < args->nr_Reducer + 2; putere++)
                {
                    char gasit = 0;
                    for (int j = 2; j < nr / 2 && gasit == 0; j++)
                    {
                        if (pow(j, putere) == nr)
                        {
                            gasit = 1;
                            if (args->size_liste_partiale[putere - 2] >= args->capacity_liste_partiale[putere - 2])
                            {
                                args->capacity_liste_partiale[putere - 2] *= 2;
                                int *ret = (int *)realloc(args->liste_partiale[putere - 2], args->capacity_liste_partiale[putere - 2] * sizeof(int));
                                if (ret != NULL)
                                {
                                    args->liste_partiale[putere - 2] = ret;
                                }
                            }

                            args->liste_partiale[putere - 2][args->size_liste_partiale[putere - 2]++] = nr;
                        }
                    }
                }
            }
        }
        else
        {
            pthread_mutex_unlock(args->mutex);
        }
    }

    pthread_barrier_wait(args->bariera);

    /*pthread_mutex_lock(args->mutex);
    printf("Thread id: %d\n", args->id_thread);
    for(int i = 0; i < args->size_liste_partiale[0]; i++)
    {
        printf("%d ", args->liste_partiale[0][i]);
    }
    printf("\n");
    pthread_mutex_unlock(args->mutex);
    */
    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    int nr_Mapper, nr_Reducer;
    char *fisier = NULL;
    get_args(argc, argv, &nr_Mapper, &nr_Reducer, &fisier);
    FILE *f0 = fopen(fisier, "r");
    int nr_fisiere;
    char *string_nr_fisiere = calloc(Len_Title, sizeof(char));
    fgets(string_nr_fisiere, Len_Title, f0);
    nr_fisiere = atoi(string_nr_fisiere);
    
    char **fisiere = calloc(nr_fisiere, sizeof(char *));
    for (int i = 0; i < nr_fisiere; i++)
    {
        fisiere[i] = calloc(Len_Title, sizeof(char));
        fgets(fisiere[i], Len_Title, f0);
        fisiere[i] = strtok(fisiere[i], "\n");
    }

    int nr_threads = maxim(nr_Mapper, nr_Reducer);
    pthread_t *threads = calloc(nr_threads, sizeof(pthread_t));
    Targ *args;
    args = calloc(nr_threads, sizeof(Targ));
    int index = 0;
    int nr_fisiere_ramase = nr_fisiere;
    
    pthread_mutex_t mutex;
    pthread_barrier_t bariera;
    pthread_mutex_init(&mutex, NULL);
    pthread_barrier_init(&bariera, NULL, nr_threads);
   
    for (int i = 0; i < nr_threads; i++)
    {
        args[i].id_thread = i;
        args[i].fisiere = fisiere;
        args[i].index = &index;
        args[i].nr_fisiere_ramase = &nr_fisiere_ramase;
        args[i].nr_Mapper = nr_Mapper;
        args[i].nr_Reducer = nr_Reducer;
        args[i].a_luat_fisier = 0;
        args[i].mutex = &mutex;
        args[i].bariera = &bariera;
        args[i].liste_partiale = calloc(nr_threads, sizeof(int *)); // alocam cate un vector catre un vector de  nr Reducer de liste pt fiecare mapper
        args[i].capacity_liste_partiale = calloc(nr_threads, sizeof(int));
        args[i].size_liste_partiale = calloc(nr_threads, sizeof(int));
        if(i < nr_Reducer)
        {
            args[i].reduce = i;
        }
        else
        {
            args[i].reduce = -1;
        }
        int r = pthread_create(&threads[i], NULL, fct, (void *)&args[i]);
        if (r)
        {
            printf("Eroare la crearea thread-ului %d\n", i);
            exit(-1);
        }
    }
    for (int i = 0; i < nr_threads; i++)
    {
        int r = pthread_join(threads[i], NULL);
        if (r)
        {
            printf("Eroare la asteptarea thread-ului %d\n", i);
            exit(-1);
        }
    }

    pthread_mutex_destroy(&mutex);
    pthread_barrier_destroy(&bariera);
    pthread_exit(NULL);
}