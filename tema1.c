#include <pthread.h>
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

#define Size 4
#define Len_Title 30
#define Capac 250

typedef struct arg
{
    char **fisiere;
    int nr_Mapper, nr_Reducer, id_thread;
    pthread_mutex_t *mutex;
    pthread_barrier_t *bariera;
    int **liste_partiale;
    int *size_liste_partiale;
    int *capacity_liste_partiale;
    int *rezultate, *nr_fisiere_ramase, *index;
    int reduce, mapper, nr_threads, capacity_rez, size_rez, prima_it;
    struct arg *vector_args;
} Targ;

char *itoa(int a)
{
    int ra = 0;
    int nr_cif = 0;
    while (a > 0)
    {
        ra = ra * 10 + a % 10;
        a /= 10;
        nr_cif++;
    }
    char *string = calloc(nr_cif + 1, sizeof(char));
    while (ra > 0)
    {
        char *lit = calloc(2, sizeof(char));
        lit[0] = ra % 10 + '0';
        lit[1] = 0;
        ra /= 10;
        strcat(string, lit);
    }
    return string;
}
int maxim(int a, int b)
{
    if (a >= b)
    {
        return a;
    }
    return b;
}

void adauga_el_nou_vector(int x, int *v, int *size, int *capacity)
{
    int i;
    for (i = 0; i < *size; i++)
    {
        if (v[i] == x)
        {
            return;
        }
    }
    if (*size >= *capacity)
    {
        *capacity = 2 * (*capacity);
        int *r = realloc(v, *capacity);
        if (r)
        {
            v = r;
        }
        else
        {
            exit(-1);
        }
    }
    v[*size] = x;
    (*size)++;
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
    if (args->prima_it == 1)
    {
        args->liste_partiale = calloc(args->nr_threads, sizeof(int *));
        args->capacity_liste_partiale = calloc(args->nr_threads, sizeof(int));
        args->size_liste_partiale = calloc(args->nr_threads, sizeof(int));
        args->size_rez = 0;
        args->capacity_rez = Capac;
        args->rezultate = calloc(args->capacity_rez, sizeof(int));
    }

    while (*(args->nr_fisiere_ramase) > 0)
    {
        pthread_mutex_lock(args->mutex);
        if (*(args->nr_fisiere_ramase) > 0 && args->mapper >= 0)
        {
            FILE *f = fopen(args->fisiere[*(args->index)], "r");
            if (f != NULL)
            {
                *(args->index) = *(args->index) + 1;
                *(args->nr_fisiere_ramase) = *(args->nr_fisiere_ramase) - 1;
            }
            pthread_mutex_unlock(args->mutex);

            int nr_valori;
            char *string = calloc(Len_Title, sizeof(char));
            fgets(string, Len_Title, f);
            nr_valori = atoi(string);
            if (args->prima_it == 1)
            {
                for (int i = 0; i < args->nr_Reducer; i++)
                {

                    int capacity = args->capacity_liste_partiale[i] = Capac;
                    args->size_liste_partiale[i] = 0;
                    args->liste_partiale[i] = calloc(capacity, sizeof(int));
                }
            }

            for (int i = 0; i < nr_valori; i++)
            {
                fgets(string, Len_Title, f);
                int nr = atoi(string);

                if (nr == 1)
                {

                    for (int putere = 2; putere < args->nr_Reducer + 2; putere++)
                    {
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
                else if (nr != 0)
                {
                    int rad = sqrt(nr);
                    if (rad * rad == nr)
                    {
                        int putere = 2;
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

                    for (int putere = 3; putere < args->nr_Reducer + 2; putere++)
                    {

                        int exista_putere = 1;

                        char gasit = 0;
                        double sup = log2(nr) / putere;
                        double inf = floor(sup);
                        sup = ceil(sup);
                        if(inf < 1)
                        {
                            inf = 2;
                        }
                        else
                        {
                            inf = floor(pow(2, inf));
                        }
                        if(sup < 1)
                        {
                            sup = 2;
                        }
                        else
                        {
                            sup = ceil(pow(2, sup));
                        }
                        

                        for (int j = inf; j <= sup && gasit == 0 && exista_putere == 1; j++)
                        {
                            double aux = pow(j, putere);
                            if (aux == nr)
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
            fclose(f);
        }
        else
        {
            pthread_mutex_unlock(args->mutex);
        }
        args->prima_it = 0;
    }

    pthread_barrier_wait(args->bariera);

   /* pthread_mutex_lock(args->mutex);
    //printf("Thread id: %d\n", args->id_thread);
    for (int i = 0; i < args->size_liste_partiale[0]; i++)
    {
        printf("%d ", args->liste_partiale[0][i]);
    }
    printf("\n");
    pthread_mutex_unlock(args->mutex);
    */

    if (args->reduce != -1)
    {
        for (int i = 0; i < args->nr_threads; i++)
        {
            if (args->vector_args[i].mapper != -1)
            {
                for (int j = 0; j < args->vector_args[i].size_liste_partiale[args->reduce]; j++)
                {
                    int x = args->vector_args[i].liste_partiale[args->reduce][j];
                    adauga_el_nou_vector(x, args->rezultate, &(args->size_rez), &(args->capacity_rez));
                }
            }
        }
    }
    char *string_fisier = calloc(Len_Title, sizeof(char));
    strcat(string_fisier, "out");
    strcat(string_fisier, itoa(args->reduce + 2));
    strcat(string_fisier, ".txt");
    FILE *fout = fopen(string_fisier, "w");
    fprintf(fout, "%d", args->size_rez);

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
        args[i].mutex = &mutex;
        args[i].bariera = &bariera;

        args[i].vector_args = args;
        args[i].nr_threads = nr_threads;
        args[i].prima_it = 1;

        if (i < nr_Reducer)
        {
            args[i].reduce = i;
        }
        else
        {
            args[i].reduce = -1;
        }

        if (i < nr_Mapper)
        {
            args[i].mapper = i;
        }
        else
        {
            args[i].mapper = -1;
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
    fclose(f0);
    pthread_mutex_destroy(&mutex);
    pthread_barrier_destroy(&bariera);
    pthread_exit(NULL);
}