#include <stdio.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>
#include <dirent.h>
#define err(str) fprintf(stderr, str)
#define max(a,b) (((a)<(b))?(a):(b))
#define min(a,b) (((a)>(b))?(a):(b))
/*Global*/
static volatile int SERVICE_KEEPALIVE;  // Service status used for run forever or terminate program
const int CAPACITY = 100;
/*----------class-----------*/
struct Cluster;
struct Worker;
/*--------------------------*/

typedef struct Control{
    int service_status;    
    int port;
    char inet[16];
    int num_worker;
    int session_timeout;
    int epoll_timeout;
    int max_epoll_event;
    int num_lock;
    int scheduling_mode;
    char root[20];
    char file_root[20];
    int hash_size;
} Control;

typedef struct Tokenizer{
    char prefix;
    char suffix;
    char no_file_token; // <H>
    char find_file_token; // <C>
} Tokenizer;

typedef struct HashNode{
    char* key;
    char* value;
} HashNode;

typedef struct HashTable{
    HashNode** items;
    int size;
    int count;
} HashTable;

typedef struct File{
    /*
    Object for transfering query-result to client
     */
    char name[50];
    unsigned char contents_b[1024];
    int size;
    int find;
} File;

typedef struct Session{
    /*
    To manage client-session 
    Includeded with "client fd", "worker id which handling sesseion", and read buffer
    */
    uintptr_t session_iid;
    int worker_id;
    char read_buffer[1024];
    int varlead_status;
    struct Cluster* cluster;
} Session;

typedef struct Job{
    /*
    Linked list for indexing job
    Working functionaly -->  func(session)
    prev:: linked list
    function:: function for processing task, in this cases, functions do read stream, find file and send stream 
    info:: args for function, managed with session object
     */
    struct Job* prev; 
    void (*function )(void* args);
    Session* info; // args for function
    int worker_id; // used when Job allocated for Worker-Queue

} Job;

typedef struct JobScheduler{
    int num_lock;
    int len;
    pthread_mutex_t rxmutex;
    int* bit;
} JobScheduler;

typedef struct JobQueue{
    /*
     Queue object for manage job-linked-list, works with two pointer
     Notifiy new-job to worker-thread  with job queue and binary semaphore
     if new job has come, submited by cluster shaped of Session. and broadcast them to all alive-work-thread
     len:: left job
     has_job:: lock for job queue. broadcast to all thread
     */
    sem_t mutex;
    pthread_mutex_t rxmutex;
    Job* front;
    Job* rear;
    int len;
} JobQueue;

typedef struct Worker{
    /*
     Child thread at the thread-pool
     Processing session functionaly
    (v0.1)Wait until new job is allocated, and it is done with one single lock
    (v0.2)Wait until new job is allcoated, and each thread have different bucket of lock
    -----------------------------------------------------------------------
     id:: logical id for worker
     cluster:: pointer for main thread-pool
     */
    pthread_t pthread;
    int id; // logical index for worker
    Job* job; // <v.0.2>private container for new job 
    struct Cluster* cluster;
} Worker;

typedef struct Cluster{
    /*
     Main thread-pool
     - Create stream
     - Create worker and manage them with worker-queue
     - Handing client fd with epoll
     - Have global lock used for idle all thread or terminate thread
     --------------------------------------------------------------    
     main_thread :: thread_t
     lock:: global lock for shotdown, idle, terminate all worker
     idle:: lock for wait signal
     wokers:: pointer array for Worker-object
     job_queue:: handler for job-liked-list
     server_fd:: server's stream fd
     serv_addr:: server socket
     port:: open port number
     inet:: open server address
     */
    struct Control* control;
    pthread_t main_thread;
    pthread_t schedule_thread;
    pthread_mutex_t lock;
    pthread_cond_t idle;
    Worker** workers;
    int num_worker;
    int* job_queue_fd;
    JobQueue job_queue;
    JobScheduler scheduler;
    int server_fd;
    struct sockaddr_in serv_addr;
    int port;
    /*stream event*/
    int stream_event_fd;
    struct epoll_event* stream_event_loop;
    /*pipe */
    int** worker_pipe;
    int* cluster_pipe;
    struct epoll_event* pipe_events;
    int pipe_epoll_fd;
    /* file hash*/
    HashTable* filetable;
} Cluster;

/*--------------------------*/

/*-----------call------------*/
struct Cluster;
struct Worker;
static int worker_init(Cluster* cluster, struct Worker** thread_p, int id);
void task_fn(void* args);
struct File* find_file_only(char* file_name);
struct Session* create_session(int  new_session_id );
static int submit_with_session(Cluster* cluster, void(*function)(void*), void* session_p);
void file_event_handler(void* args);
void pre_processing_handler(void* args);
void word_count_handler(void* args);
/*--------------------------*/
/*read& write socket */
/*--------------file hash---------*/

void free_item(HashNode* item){

    free(item->key);
    free(item->value);
    free(item);
}

void free_table(HashTable* table){
    for (int i=0; i < table->size; i++){
        HashNode* item = table->items[i];
        if (item != NULL)
            free_item(item);
    }
    free(table->items);
    free(table);
}

unsigned long hash_function(char *str){
    unsigned long i = 0;
    for (int j = 0; str[j]; j++)
        i += str[j];
    return i % CAPACITY;
}

static unsigned int hash_function2(char* key)
{
	unsigned int h = 5381;
	while(*(key++))
		h = ((h << 5) + h) + (*key);
	return h;
}

HashTable* hash_init( int size){
    HashTable* table = (HashTable* )malloc(sizeof(HashTable));
    table->size = size;
    table->count = 0;
    table->items = (HashNode**)calloc(table->size, sizeof(HashNode* ));
    for (int i=0; i < table->size; i++)
        table -> items[i] = NULL;
    return table;
}

struct HashNode* create_item(char* key, char* value){
    HashNode* item = (HashNode* )malloc(sizeof(HashNode));
    item ->key = (char* )malloc(strlen(key)+1);
    item -> value = (char* )malloc(strlen(value)+1);
    strcpy(item->key, key);
    strcpy(item->value, value);
    return item;
}



static void update_hash(HashTable* table, char* key, char* value){
    HashNode* item = create_item(key, value);
    int idx = hash_function2(key);
    HashNode* cur = table->items[idx];
    if (cur == NULL){
        if (table -> count ==table->size){
            free_item(item);
            return ;
        }
        table -> items[idx] = item;
        table -> count ++;
    }
    else {
        if (strcmp(cur->key, key) == 0){
            strcpy(table->items[idx] -> value, value);
            return ;
        }
    }
}

char* search_hash(HashTable* table, char* key){
    int idx = hash_function2(key);
    HashNode* item = table -> items[idx];
    if (item != NULL){
        if (strcmp(item-> key, key) == 0){
            return item -> value;
        } 
    }
    return NULL;
}

static int file_io(Cluster* cluster, char* file_name){
    char file_path[1024] = {0};
    char value[2048] = {0};
    strcpy(file_path, cluster->control ->file_root);
    strcat(file_path, file_name);
    FILE* fptr;
    fptr = fopen(file_path, "r");
    if (fptr ==NULL)
        return -1;

    fread(value, 1, sizeof(value), fptr);
    update_hash(cluster->filetable, file_name, value);
    return 0;
}

struct File* find_file_only(char* file_name){
    File* file;
    char file_path[2048] = {0};
    char* path = "./server/book_file/";
    snprintf(file_path, strlen(path)+1, "%s", path);
    strcat(file_path, file_name);
    FILE* fptr;
    fptr = fopen(file_path, "r");
    if (fptr==NULL)
        return NULL;

    file = (struct File* )malloc(sizeof(struct File));
    fread((file->contents_b), 1, sizeof(file->contents_b), fptr);
    snprintf(file->name,50, "%s", file_name);
    file->find = 1;
    fclose(fptr);
    return file;
}

/*-------------------------------*/
/*------------Job queue------------*/

static int job_queue_init(Cluster* cluster, JobQueue* job_queue){
    cluster -> job_queue_fd = (int* )malloc(2 *sizeof(int));
    pipe(cluster->job_queue_fd);
    struct epoll_event register_ ;
    register_.events = EPOLLIN | EPOLLET;
    register_.data.fd = cluster -> job_queue_fd[0];
    if (epoll_ctl(cluster->pipe_epoll_fd, EPOLL_CTL_ADD, cluster->job_queue_fd[0], &register_)<0){
        err("jobqueue fd init error\n");
        return -1;
    }
    job_queue -> len = 0;
    job_queue -> front = NULL;
    job_queue -> rear = NULL;
    sem_init(&job_queue->mutex, 0, 0);
    pthread_mutex_init(&job_queue->rxmutex, NULL);
    return 0;
}

static void bit_post(JobScheduler* scheduler, int self){
    pthread_mutex_lock(&scheduler->rxmutex);
    scheduler->bit[self] = 1;
    scheduler->len ++;
    pthread_mutex_unlock(&scheduler->rxmutex);
}

static void bit_work(JobScheduler* scheduler, int self){
    pthread_mutex_lock(&scheduler->rxmutex);
    scheduler->bit[self] = 0;
    scheduler -> len --;
    pthread_mutex_unlock(&scheduler->rxmutex);
}
static int check_idle(JobScheduler* scheduler){
    int res = -1;
    pthread_mutex_lock(&scheduler->rxmutex);
    if (scheduler->len >0){
        res = 0;
    }
    pthread_mutex_unlock(&scheduler->rxmutex);
    return res;
}

static void push_back(JobQueue* job_queue, struct Job* new_job){
    pthread_mutex_lock(&job_queue->rxmutex);
    new_job->prev = NULL;
    
    if (job_queue->len==0){
        job_queue->front = new_job;
        job_queue->rear = new_job;
    }
    if (job_queue->len>=1){
        job_queue->rear->prev = new_job;
        job_queue->rear = new_job;
    }
    job_queue -> len ++ ;
    pthread_mutex_unlock(&job_queue->rxmutex);
}

static void push_back_worker(Worker* worker , struct Job* new_job){
    worker->job = new_job;
}

static struct Job* pop_front_worker(Worker* self){
    Job* new_job = self->job;
    return new_job;
}

static struct Job* pop_front(JobQueue* job_queue){
    
    pthread_mutex_lock(&job_queue-> rxmutex);
    Job* front = job_queue ->front;
    if (job_queue->len==1){
        job_queue->front = NULL;
        job_queue->rear = NULL;
        job_queue -> len = 0;
    } 
    else if (job_queue->len >=2){
        job_queue -> front = front->prev;
        job_queue -> len -- ;
    }
    pthread_mutex_unlock(&job_queue->rxmutex);
    return front;
}

static int pipe_init(Cluster* cluster, int num_worker){
   
    cluster->worker_pipe = (int** )malloc(num_worker * sizeof(int* ));
    cluster->cluster_pipe = (int* )malloc(2 * sizeof(int));
    pipe(cluster->cluster_pipe);
    cluster ->pipe_epoll_fd = epoll_create(1024);
    cluster ->  pipe_events = (struct epoll_event* )malloc(1024*sizeof(struct epoll_event));
    struct epoll_event pipe_register;
    pipe_register.events = EPOLLIN | EPOLLET;
    pipe_register.data.fd = cluster -> cluster_pipe[0];
    if (epoll_ctl(cluster->pipe_epoll_fd, EPOLL_CTL_ADD, cluster ->cluster_pipe[0], &pipe_register)<0){
        err("pipe init error \n");
    }
    
    for (int i=0; i<num_worker; i++){
        cluster->worker_pipe[i] = (int* )malloc(2 * sizeof(int));
        pipe(cluster->worker_pipe[i]);
    } 
    return 0; 
}    

static int scheduling_handler(Cluster* cluster, Job* new_job){
    if (new_job ==NULL){
        return -1;
    }
    char notify[2] ={'1'};
    pthread_mutex_lock(&cluster ->scheduler.rxmutex);
    if (cluster->scheduler.len <=0){
        pthread_mutex_unlock(&cluster ->scheduler.rxmutex);
        err("Job Scheduler error check Job queue and push & pop methods \n") ;
        return -1;
    }
    for (int i=0; i< cluster->num_worker; i++){
       if (cluster -> scheduler.bit[i] ==1 && SERVICE_KEEPALIVE){
            cluster -> scheduler.bit[i] = 0;
            cluster -> scheduler.len --;
            pthread_mutex_unlock(&cluster -> scheduler.rxmutex);
            push_back_worker(cluster->workers[i], new_job);
            write(cluster -> worker_pipe[i][1], notify, sizeof(notify));
            return 0;
        } 
    }
    return 1;
}

static void* cluster_manager(Cluster* cluster){
    printf("===  Cluster Manager Starot on Thread === \n");
    int batch;
    char new[2];
    char busy_msg[100] = {0};
    int schedule_status;
    snprintf(busy_msg, 100, "<nsf> Server Busy Now, redirect later");
    while (SERVICE_KEEPALIVE){
        batch = epoll_wait(cluster->pipe_epoll_fd, cluster -> pipe_events, 1024, cluster->control->session_timeout);     
        if (batch < 0){
            err("--manager batch fd error found \n");
            continue;
        }
        for (int i =0; i <batch; i++){
            if (cluster->pipe_events[i].data.fd == cluster->job_queue_fd[0]){
                read(cluster->job_queue_fd[0], new, sizeof(new));
                new[0] = '\0';
                Job* new_job = pop_front(&cluster -> job_queue);
                schedule_status = scheduling_handler(cluster, new_job);
                if (schedule_status==0){
                }
                else if (schedule_status==1){
                    Session* session = new_job->info;

                    send(session->session_iid, busy_msg, strlen(busy_msg),0); 
                    printf("\nWorker busy now close session[%ld] \n", session->session_iid);
                    epoll_ctl(cluster -> stream_event_fd, EPOLL_CTL_DEL, session->session_iid, NULL);
                }
                else {
                }
            }
        }
    }
}

int is_valid_fd(int fd)
{
    return fcntl(fd, F_GETFL) != -1 || errno != EBADF;
}

static void* worker_handler(Worker* worker){
    char worker_name[16] = {0};
    snprintf(worker_name, 16, "worker#%d", worker->id);
    Cluster* cluster = worker -> cluster;
    int self = worker ->id;
    char stage[2];
    while (SERVICE_KEEPALIVE){
        int status = read(cluster->worker_pipe[self][0], stage, sizeof(stage));
        if (status <0){
            printf("read error while handling worker thread \n");
            continue;
        }
        void(* fn)(void*);
        void* args;
        Job* new_job = pop_front_worker(worker);
        if (new_job && is_valid_fd(new_job->info->session_iid)){
            new_job->info->worker_id = self;
            fn = new_job->function;
            args = new_job->info;
            if (fn ==NULL){
            }
            if (args ==NULL){
            }
            fn(args);
            free(new_job->info);
            free(new_job);
        }
        bit_post(&cluster->scheduler, self);
    }
}

static int add_event(int epoll_fd, int client_fd, int args){
    /* set as non _blocking */
    struct epoll_event ev;
    ev.events = args;
    ev.data.fd = client_fd;
    if( epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) <0 )
        return -1;
    return 0;
}

static int add_event_non_block(int epoll_fd, int client_fd, int args){
    int flags =fcntl(client_fd, F_GETFL, 0);
    if (flags <0)
        return -1;
    if (fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) <0)
        return -1;
    
    struct epoll_event ev;
    ev.events = args;
    ev.data.fd = client_fd;
    if( epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) <0 )
        return -1;
    return 0;
}

static int del_event(int epoll_fd, int client_fd, int args){
    struct epoll_event ev;
    ev.events = args;
    ev.data.fd = client_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL);
    return 0;
}


static void* listen_handler(Cluster* cluster){
    /*at cluster do listen handing*/
    printf("=== Listen Handler Working on Thread ===\n");
    Control* control = cluster -> control;
    int batch;
    char notify[2] = {1};
    struct epoll_event* stream_event_loop = cluster -> stream_event_loop;
    while (SERVICE_KEEPALIVE && cluster->server_fd >0){
        batch = epoll_wait(cluster -> stream_event_fd, stream_event_loop, control->max_epoll_event, control->session_timeout);
        if (batch <0){
            err("Listen Handelr error \n");
            continue;
        }
        for (int i =0; i<batch; i++){
            if (stream_event_loop[i].data.fd == cluster ->server_fd){
                /*
                if (check_idle(&cluster->scheduler) < 0){
                    printf("Not Allowed accept, no Idle worker \n");
                    continue;
                }
                */
                int new_client;
                int session_len;
                struct sockaddr_in client_addr;
                session_len = sizeof(client_addr);
                new_client = accept(cluster -> server_fd, (struct sockaddr* )&client_addr, (socklen_t* )&session_len);

                if(new_client <0){
                    err("client accept error \n");
                    continue;
                }
                /*
                if (add_event(cluster->stream_event_fd, new_client, EPOLLIN | EPOLLET) <0){
                    err("at listen handler :: add event failed\n");
                }
                */
                if (add_event_non_block(cluster->stream_event_fd, new_client, EPOLLIN | EPOLLET) <0){
                    err("at listen handler :: add event failed\n");
                }
            }
            else {
                int str_len;
                int new_client = stream_event_loop[i].data.fd;
                Session* session;
                session = create_session(new_client);
                if (session ==NULL){
                    //session = create_session(new_client);
                    printf(" Session could not allocat error ");
                }
                session->cluster = cluster;
                str_len = read(session -> session_iid, &session->read_buffer, sizeof(session->read_buffer)-1);
                if (str_len <0){
                    printf("close session on shutdown[%d]\n", new_client);
                    del_event(cluster->stream_event_fd, new_client, 0);
                    close(new_client);
                }
                
                else if (str_len==0){
                    printf("close session timeout[%d] \n", new_client);
                    del_event(cluster->stream_event_fd, new_client, 0);
                    close(new_client);
                }
                else {
                    session -> read_buffer[str_len] = '\0';
                    if (strncmp(session->read_buffer, "<fnd>", 5)==0){
                        printf("API:: find \n");
                        submit_with_session(cluster, file_event_handler, (void*)(uintptr_t)session);
                        write(cluster -> job_queue_fd[1], notify, sizeof(notify));
                    }
                    else if (strncmp(session->read_buffer, "<pps>", 5)==0){
                        printf("API:: pre-processing\n");
                        submit_with_session(cluster, pre_processing_handler, (void*)(uintptr_t)session);
                        write(cluster -> job_queue_fd[1], notify, sizeof(notify));
                    }
                    else if (strncmp(session->read_buffer, "<wdc>",5)==0){
                        printf("API:: word-counter\n");
                        submit_with_session(cluster, pre_processing_handler, (void*)(uintptr_t)session);
                        write(cluster -> job_queue_fd[1], notify, sizeof(notify));
                    }
                    else{
                        printf("API:: default -> find file \n");
                        if (submit_with_session(cluster,task_fn, (void*)(uintptr_t)session) <0)
                            err(" could not submit session to job queue \n");
                        else{
                            write(cluster-> job_queue_fd[1], notify, sizeof(notify));
                        }
                    }
                }
            }
        }
    }
}

static int submit_with_session(Cluster* cluster, void(*function)(void*), void* session_p){
    /*
     Create session and push job to job-queue
    
     - Create job which container for processing query
     - Create session wihch manage life-cycle of seesion, client fd, read-buffer
     - Pusch back job (contianer) into queue
     */
    Job* new_job;
    new_job =(struct Job*)malloc(sizeof(struct Job));
    if (new_job==NULL){
        err("at submit():: could not allocate memory for new job\n");
        return -1;
    }
    Session* session;
    session = session_p;
    new_job->function = function;
    new_job->info = session;
    push_back(&cluster->job_queue, new_job);
    return 0;
}


struct Session* create_session(int new_session_id ){
    Session* session;
    session = (struct Session* )malloc(sizeof(struct Session));
    if (session ==NULL){
        err("at submit():: could not allocate memory for new session\n");
        return NULL;
    }
    session->session_iid = (uintptr_t)new_session_id;
    session->read_buffer[0] = '\0';
    return session;
}



void task_fn(void* args){
    /*
     task fn
     - create file fd
     - find file from server
     - send file to client */
    Session* session = (Session* )args;
    File* file;
    printf("Thread #%d (%u)  Working on session[%ld] \n", session->worker_id , (int)pthread_self(), session->session_iid);
    printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session->worker_id, session->session_iid,  session->read_buffer);
    file = find_file_only(session->read_buffer);
    if (file==NULL){
        char no_file_msg[1024] = {0};
        snprintf(no_file_msg, 1024, "<csf>such a file name doesn't exists");
        send(session->session_iid, no_file_msg, strlen(no_file_msg),0); 
        printf("\t:NO FILE::  T[%d] send msg to Session[%ld] \n",session->worker_id ,session->session_iid);
     }  
     else{
        send(session->session_iid, file->contents_b, sizeof(file->contents_b),0);
        printf("\t: T[%d] send msg to Session[%ld] \n",session->worker_id ,session->session_iid);
        free(file);
     }
}

void file_event_handler(void* args){
    Session* session = (Session* )args;
    printf("---debug--- dev \n"); 
    printf("Thread #%d (%u)  Working on session[%ld] \n", session->worker_id , (int)pthread_self(), session->session_iid);
    printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session->worker_id, session->session_iid,  session->read_buffer);
    char file_name[100];
    strncpy(file_name, session->read_buffer + 5, sizeof(session->read_buffer)-1);
    char* value;
    value = search_hash(session->cluster->filetable, file_name);
    if (value != NULL){
        char buf[2048];
        strcpy(buf, value);
        send(session->session_iid, buf, sizeof(buf), 0);
        printf("\t: Cashe Hit::  T[%d] send msg to Session[%ld] \n",session->worker_id ,session->session_iid);
        return ;
    }
    else{
        if (file_io(session->cluster, file_name) ==0){
            value = search_hash(session->cluster->filetable, file_name);
            if (value != NULL){
                char buf[2048];
                strcpy(buf, value);
                send(session->session_iid, buf, sizeof(buf), 0);
                printf("\t: Cache Miss::  T[%d] send msg to Session[%ld] \n",session->worker_id ,session->session_iid);
                return ;
            }
        }
        char no_file_msg[1024] = {0};
        snprintf(no_file_msg, 1024, "<csf>such a file name doesn't exists");
        send(session->session_iid, no_file_msg, strlen(no_file_msg),0); 
        printf("\t: NO FILE::  T[%d] send msg to Session[%ld] \n",session->worker_id ,session->session_iid);
    }
}


void word_count_handler(void* args){
    Session* session = (Session* )args;
    printf("---debug--- dev \n"); 
    printf("Thread #%d (%u)  Working on session[%ld] \n", session->worker_id , (int)pthread_self(), session->session_iid);
    printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session->worker_id, session->session_iid,  session->read_buffer);
}

void pre_processing_handler(void* args){
    Session* session = (Session* )args;
    printf("---debug--- dev \n"); 
    printf("Thread #%d (%u)  Working on session[%ld] \n", session->worker_id , (int)pthread_self(), session->session_iid);
    printf("\t: T[%d] recv msg from Session[%ld] ::  %s \n",session->worker_id, session->session_iid,  session->read_buffer);
}

static int job_scheduler_init(JobScheduler* scheduler, int num_lock){
    /*
     */
    scheduler->len = num_lock;
    scheduler->num_lock = num_lock;
    scheduler -> bit = (int* )malloc(num_lock * sizeof(int ));
    for (int i=0; i < num_lock; i++){
        scheduler ->bit[i] = 1;
    }
    pthread_mutex_init(&scheduler->rxmutex, NULL);
    return 0;
}

static int worker_init(Cluster* cluster, struct Worker** thread, int id){
    *thread = (struct Worker*)malloc(sizeof(struct Worker));
    if (thread==NULL){
        err("worker_init():: allocate worker on memory failed");
        return -1;
    }
    (*thread) -> cluster = cluster;
    (*thread) -> id = id;
    pthread_create(&(*thread)->pthread, NULL, (void * (*)(void* )) worker_handler, (*thread));
    pthread_detach((*thread)->pthread);
    return 0;
}

static int set_event_loop(Cluster* cluster, int epoll_fd, int fd, int max_event){
    /* high level api for epol */
    epoll_fd =  epoll_create(max_event);
    if (epoll_fd <0){
        err("set event loop error \n");
    }
    struct epoll_event event_register;
    event_register.events = EPOLLIN | EPOLLET;
    event_register.data.fd = fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event_register)<0){
        err("set event loop ctl error \n");
    }
    return 0;
}

static int epoll_init(Cluster* cluster, int server_fd){
    cluster -> stream_event_fd = epoll_create(cluster->control->max_epoll_event);
    if (cluster -> stream_event_fd <0){
        err("epoll init error \n");
    }
    cluster -> stream_event_loop = (struct epoll_event* )malloc(cluster->control->max_epoll_event * sizeof(struct epoll_event));
    if (cluster->stream_event_loop ==NULL){
        err("stream event loop not allocated at memory \n");
        return -1;
    }
    struct epoll_event event_register;
    event_register.events = EPOLLIN | EPOLLET;
    event_register.data.fd = server_fd;
    if (epoll_ctl(cluster->stream_event_fd, EPOLL_CTL_ADD, server_fd, &event_register)<0){
        err("stream could not register to epoll \n");
    }
    return 0;
}

static int async_stream_init(Cluster** cluster_p){
    Cluster* cluster = (*cluster_p);
    cluster->port = cluster->control->port;
    int opt = 1;
    int server_fd;
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd <0){
        perror("stream_init() scoket create failed\n");
        return -1;
    } 

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
        perror("set socket opt\n");
        return -1;
    }
    cluster->serv_addr.sin_family=AF_INET;
    cluster->serv_addr.sin_addr.s_addr = INADDR_ANY;
    cluster->serv_addr.sin_port = htons(cluster->port);
    if (bind(server_fd, (struct sockaddr*)&(cluster->serv_addr), sizeof((cluster->serv_addr)))<0){
        err("stream_init : Bind Failed\n");
        return -1;
    }

    if (listen(server_fd, 3)<0){
        perror("listen Failed\n");
        return -1;
    }
    cluster -> server_fd = server_fd;
    if (epoll_init(cluster, server_fd)<0){
        perror("set epoll Failed\n");
        return -1;
    }
    return 0;
}

struct Cluster* cluster_init(Control* control){
    /*
       Allocate object to memory
       First create all the object at the process
       If all works done, give them to thread
       1) create cluster
       2) create job-queue
       3) create stream
       4) create worker -> not a terminate condition */
    Cluster* cluster;
    cluster = (struct Cluster* )malloc(sizeof(struct Cluster ));
    if (cluster==NULL){
        err("cluster_init():: allocatate cluster on memory failed\n");
        return NULL;
    } 
    cluster->control = control;
    cluster->num_worker = control->num_worker;
    int num_worker = control->num_worker;
    /*pipeline for each worker init */
    if (pipe_init(cluster, num_worker)<0){
        free(cluster);
        return NULL;
    }
    /*create job queue, terminate condition */
    if (job_queue_init(cluster, &cluster->job_queue) < 0){
        err("could not allocate job queue ");
        free(cluster);
        return NULL;
    }

    if (job_scheduler_init(&cluster->scheduler, cluster->num_worker) <0){
        free(cluster);
        return NULL;
    } 


    cluster->workers = (struct Worker** )malloc(num_worker * sizeof(struct Worker* ));
    if (cluster->workers==NULL){

        err("cluster_init():: allocatate worker  on memory failed\n");
        return NULL;
    }
    /*create stream on cluster*/
    if (async_stream_init(&cluster) < 0 ){
        err("|_ create stream Failed \n");
        return NULL;
    }
    /*create lock*/ 
    pthread_mutex_init(&(cluster->lock), NULL);
    pthread_cond_init(&cluster->idle, NULL);
    /*create worker thread */
    for (int i=0; i<num_worker; i++){
        worker_init(cluster, &cluster->workers[i], i );
        printf("cluster init : Work thread %d created\n", i);
    }
    /* file hash table init */
    cluster->filetable = hash_init(control->hash_size);
    if (cluster->filetable ==NULL){
       err("|_ create Hash table Failed \n");
    }
    return cluster;
}


static int control_init(Control* control){
    /*
     I/O from server/control.txt
     */
    FILE* fptr;
    char* root = "./server/control.txt";
    printf("control path (%s) \n", root); 
    fptr = fopen(root, "a+");
    char key[20];
    char value[20];
    while ( fscanf(fptr, "%s\t%s\n", key, value) != EOF ){
        if (strcmp(key, "port")==0){
            control->port = atoi(value);
        }
        else if (strcmp(key, "inet")==0){
        }
        else if (strcmp(key, "num_worker")==0){
            control->num_worker = atoi(value);
        }
        else if (strcmp(key, "session_timeout")==0){
            control->session_timeout = atoi(value);
        }
        else if (strcmp(key, "epoll_timeout")==0){
            control->epoll_timeout = atoi(value);
        }
        else if (strcmp(key, "max_epoll_event")==0){
            control -> max_epoll_event = atoi(value);
        }
        else if (strcmp(key, "root")==0){
            strcpy(control->root, value);
        }
        else if (strcmp(key, "ececutor.cores.scheduling")){
            control -> scheduling_mode = atoi(value);
        }
        else if(strcmp(key, "hash_size")){
            control -> hash_size = atoi(value);
        }
        else if(strcmp(key, "file_root")){
            strcpy(control->file_root, value);
        } 
    }
    return 0;
}

static int free_queue(JobQueue* jobqueue){
    /*
    for (int i=0; i<jobqueue->len; i++){
        Job* front = pop_front(jobqueue);
        free(front);
    }
    */
    sem_destroy(&jobqueue->mutex);
    pthread_mutex_destroy(&jobqueue->rxmutex);
    free(jobqueue);
    return 0;
}

static int free_worker(Worker* worker){
    if (worker->job != NULL){
        free(worker->job);
    }
//    pthread_mutex_destroy(worker.pthread);
    free(worker);
    return 0;
}

static int free_cluster(Cluster* cluster){
    for (int i =0; i < cluster->num_worker; i++){
        Worker* worker = cluster->workers[i];
        if (worker != NULL)
            free_worker(worker);
    }
    free(cluster);
    return 0;
}

static int flush_all(Cluster* cluster){
    SERVICE_KEEPALIVE = 0;
    /* Job Scheduler */
    free(&cluster->scheduler);
    free_queue(&cluster->job_queue);
    free(cluster->control);
    free_cluster(cluster);
    return 0;
}

int main(){
    Control* control;
    if ( control_init(control) <0)
        return -1;

    SERVICE_KEEPALIVE =1;
    Cluster* cluster = cluster_init(control);
    pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) listen_handler, cluster);
    pthread_create(&(cluster->schedule_thread), NULL, (void * (*)(void* )) cluster_manager, cluster);
    pthread_detach(cluster->main_thread);
    pthread_detach(cluster->schedule_thread);
    /* service start */
    while ( SERVICE_KEEPALIVE){
        continue;
        }
    printf("----Service Stop----");
    return 0;
}
