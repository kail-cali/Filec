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

#include <pthread.h>
#include <dirent.h>
#define err(str) fprintf(stderr, str)



#define max(a,b) (((a)<(b))?(a):(b))
#define min(a,b) (((a)>(b))?(a):(b))

/*Global*/

static volatile int SERVICE_KEEPALIVE;  // Service status used for run forever or terminate program

/*----------class-----------*/

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


} Control;
/*
typedef struct Event{
    int event_id;
    struct epoll_event event;
    struct sockaddr_in client_addr;

} Event;
*/

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
    
} Session;

typedef struct BinSemaphore{
    /*
     Binary-semaphore for handling thread and lock
     0: there is no job
     1: new jobs have created
     */
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int v;
} BinSemaphore;


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
    Session* info;

    int worker_id; // used when Job allocated for Worker-Queue

} Job;

typedef struct JobScheduler{
    pthread_mutex_t rxmutex;

    int num_lock;
    int len;
    BinSemaphore** has_job;

} JobScheduler;

typedef struct JobQueue{
    /*
     Queue object for manage job-linked-list, works with two pointer
    
     Notifiy new-job to worker-thread  with job queue and binary semaphore
     
     if new job has come, submited by Cluster shaped of Session. and broadcast them to all alive-work-thread


     len:: left job
     has_job:: lock for job queue. broadcast to all thread
     
     */
    pthread_mutex_t rxmutex;
    Job* front;
    Job* rear;

    int len;
    BinSemaphore* has_job;

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

    int semaphore_id; // logical index for mutex lock same as id
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

     num_worker:: number of worker thread, currently it is static from start
     num_alive_worker:: number of alive but not-working thread
     num_working_worker:: number of working thread

     job_queue:: handler for job-liked-list

     server_fd:: server's stream fd
     serv_addr:: server socket
     port:: open port number
     inet:: open server address

     
     */
    struct Control* control;

    pthread_t main_thread;
    pthread_t schedule_thread;
    pthread_t pipe_thread;

    pthread_mutex_t lock;
    pthread_cond_t idle;

    Worker** workers;
    int num_worker;

    JobQueue job_queue;
    JobQueue worker_queue; // alive worker FIFO

    JobScheduler scheduler;

    int server_fd;
    struct sockaddr_in serv_addr;
    int port;

    int pipe_fd[2];

} Cluster;


/*--------------------------*/

/*-----------call------------*/
struct Cluster;
struct Worker;

static int worker_init(Cluster* cluster, struct Worker** thread_p, int id);


void task_fn(void* args);


static void bsem_post(BinSemaphore* bsem_p);



struct File* find_file_only(char* file_name);

static void push_back(JobQueue* job_queue_p, BinSemaphore* has_job, struct Job* new_job);

struct Session* create_session(int  new_session_id );
static int submit_with_session(Cluster* cluster, void(*function)(void*), void* session_p);
static int semaphore_init(JobScheduler* scheduler, struct BinSemaphore** node);
/*--------------------------*/


static void bsem_init(BinSemaphore* bsem_p, int value){

    pthread_mutex_init(&(bsem_p->mutex), NULL);
    pthread_cond_init(&(bsem_p->cond),NULL);
    bsem_p->v = value;
}

static void bsem_reset(BinSemaphore* bsem_p){
    bsem_init(bsem_p, 0);
}

static void bsem_post(BinSemaphore* bsem_p){
    pthread_mutex_lock(&bsem_p->mutex);
    bsem_p->v = 1;
    pthread_cond_signal(&bsem_p->cond);
    pthread_mutex_unlock(&bsem_p->mutex);
}

static void wait(BinSemaphore* bsem_p){
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1){
        pthread_cond_wait(&bsem_p->cond, &bsem_p->mutex);
    }
    bsem_p->v = 0;
    pthread_mutex_unlock(&bsem_p->mutex);

}


/*--------------------------*/



static int job_queue_init(JobQueue* job_queue_p){
    job_queue_p->len =0;
    job_queue_p->front = NULL;
    job_queue_p->rear = NULL;
    job_queue_p->has_job = (struct BinSemaphore* )malloc(sizeof(struct BinSemaphore ));
    
    if (job_queue_p->has_job == NULL){
        err("job_queue_init():: could not allocate memory for binary semaphore in job queue");
        return -1;
    }

    pthread_mutex_init(&(job_queue_p->rxmutex), NULL);
    bsem_init(job_queue_p->has_job, 0);
    
    return 0;
}

// @ job_queue_init.create
static int worker_queue_init(Cluster* cluster, JobQueue* worker_queue, int num_worker){
    /*
     */
    if (  job_queue_init(worker_queue) <0){
        return -1;
    }

    for (int i=0; i< num_worker; i++){

     Job* new_job;
     new_job =(struct Job*)malloc(sizeof(struct Job));
     new_job->worker_id = i;
     push_back(&cluster->worker_queue, cluster->worker_queue.has_job , new_job);
    } 

    return 0;
}

static void push_back(JobQueue* job_queue, BinSemaphore* has_job, struct Job* new_job){
    
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
    bsem_post(has_job);

    pthread_mutex_unlock(&job_queue->rxmutex);
}

static void push_back_worker(JobScheduler* scheduler,Worker* worker ,BinSemaphore* has_job, struct Job* new_job){
    
    pthread_mutex_lock(&scheduler->rxmutex);
    worker->job = new_job;
    bsem_post(has_job);

    pthread_mutex_unlock(&scheduler->rxmutex);
    

}

static struct Job* pop_front_worker(Worker* self){
    Job* new_job = self->job;
    return new_job;
}

static struct Job* pop_front(JobQueue* job_queue_p, BinSemaphore* has_job ){
    pthread_mutex_lock(&job_queue_p-> rxmutex);
    Job* front_job = job_queue_p -> front;
    if (job_queue_p ->len ==0){

    }
    else if (job_queue_p->len==1){
        job_queue_p->front = NULL;
        job_queue_p->rear = NULL;
        job_queue_p -> len = 0;
    }
    else if (job_queue_p->len >= 2){
        job_queue_p -> front = front_job->prev;
        job_queue_p -> len --;
        bsem_post(has_job);
    }
    pthread_mutex_unlock(&job_queue_p->rxmutex);
    return front_job;

}

static void* worker_do_passive(Worker* worker){
    /*
     <v0.2> scheduling featrue init
     */
    
    char worker_name[16] = {0};
    snprintf(worker_name, 16, "worker#%d", worker->id);
    Cluster* cluster =  worker->cluster;
    int who = worker->semaphore_id;
    while (SERVICE_KEEPALIVE){
        wait(cluster->scheduler.has_job[who]);
        if (SERVICE_KEEPALIVE){
            void(* fn)(void*);
            void* args;
            Job* new_job = pop_front_worker(worker); 
            if (new_job){

                new_job->info->worker_id = who;
                fn = new_job->function;
                args = new_job->info;  
                // call method for fn
                fn(args);    
                free(new_job->info);
                free(new_job);
            }

   //         printf("sleep - latnecy::thraed[%s]  \n", worker_name);
//            sleep(10);
 //           printf("wake up- latnecy::thraed[%s] \n", worker_name);
            char idle_worker_id[2];
            idle_worker_id[0] = who +'0';
            write(cluster->pipe_fd[1], idle_worker_id, 2) ; 
        }

    }


}



static void* scheduler_do(Cluster* cluster){
    printf("== cluster start scheduling on thread== \n");
    //JobScheduler* scheduler = cluster>scheduler;
    while (SERVICE_KEEPALIVE){
        wait(cluster->job_queue.has_job);
        if (SERVICE_KEEPALIVE){
 
            Job* new_job = pop_front(&cluster->job_queue, cluster->job_queue.has_job);
            wait(cluster->worker_queue.has_job); 
            if (SERVICE_KEEPALIVE){
                Job* worker = pop_front(&cluster->worker_queue, cluster->worker_queue.has_job);
                int who = worker->worker_id;
                push_back_worker(&cluster->scheduler, cluster->workers[who], cluster->scheduler.has_job[who], new_job);
                
            }
            
        }
    }
}


static void* pipeline(Cluster* cluster){
    printf("== cluster start pipe  on thread== \n");

    Control* control = cluster->control;
    
    int pipe_epoll_fd = epoll_create(cluster->num_worker);
    if( pipe_epoll_fd <0 ){
        err(" pipe epoll init error \n");
    }
    pipe(cluster->pipe_fd);
    
    fcntl(cluster->pipe_fd[0], F_SETFL, O_NONBLOCK);

    struct epoll_event event;
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = cluster->pipe_fd[0];
    if ( epoll_ctl(pipe_epoll_fd, EPOLL_CTL_ADD, cluster->pipe_fd[0], &event)<0 ){
        err("pipe epoll error \n");
    }
    struct epoll_event pipe_events[cluster->num_worker];
    
    int alives;
    char who[10];
    while (SERVICE_KEEPALIVE){
        alives = epoll_wait(pipe_epoll_fd, pipe_events, cluster->num_worker, control->session_timeout);
        if (alives <0){
            continue;
        } 
        for (int i=0; i<alives; i++){
            
            if (pipe_events[i].data.fd==cluster->pipe_fd[0]){
                // read from fd 
                read(cluster->pipe_fd[0], who, sizeof(who));       
                // post worker queue
                Job* new_job;
                new_job =(struct Job*)malloc(sizeof(struct Job));
                new_job->worker_id = atoi(who);
                push_back(&cluster->worker_queue, cluster->worker_queue.has_job , new_job);
                }

        }
    }

}


static void* cluster_do(Cluster* cluster){
    printf("\n==Cluster working on thread== \n");
    Control* control = cluster->control;

    /*<D>epoll init temp */
    int epoll_fd = epoll_create(control->max_epoll_event);
    if (epoll_fd <0 ){
        err("epoll init error\n ");

    }

    struct epoll_event event;

    event.events = EPOLLIN | EPOLLET;
    event.data.fd = cluster->server_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, cluster->server_fd, &event) < 0){
        err("epoll init error\n");
    }

    struct epoll_event epoll_events[control->max_epoll_event];
    int batch4event;
    
    while (SERVICE_KEEPALIVE && cluster->server_fd >0){
        batch4event = epoll_wait(epoll_fd, epoll_events, control->max_epoll_event, control->session_timeout );
        if (batch4event <0){
            continue;

        }

        for (int i =0; i <batch4event; i++){
            if (epoll_events[i].data.fd==cluster->server_fd){
                int new_session;
                int session_len;
                /*<D> epoll init tmp*/
                struct sockaddr_in client_addr;

                session_len = sizeof(client_addr);
                new_session = accept(cluster->server_fd, (struct sockaddr* )&client_addr, (socklen_t* )&session_len);
                
                int flags = fcntl(new_session, F_GETFL);

                flags |= O_NONBLOCK;
                if (fcntl(new_session, F_SETFL, flags)<0){
                    printf("while creating new session[%d] fcntl() error \n", new_session);
                    continue;
                }
                if (new_session <0){
                    continue;
                }
                struct epoll_event event;
                event.events = EPOLLIN | EPOLLET;
                event.data.fd = new_session;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_session, &event)<0){
                    err("epoll ctl error\n");
                    continue;
                }

            }
             else {
                 int str_len;
                 int new_session = epoll_events[i].data.fd;
                 Session* session;
                 session = create_session(new_session);

                 str_len = read(session->session_iid, session->read_buffer, sizeof(session->read_buffer));
                
                 if (str_len==0){
                     printf("close session timeout [%d]\n", new_session);
                     close(new_session);
                     epoll_ctl(epoll_fd, EPOLL_CTL_DEL, new_session, NULL);

                 }
                 else{
                    submit_with_session(cluster, task_fn, (void* )(uintptr_t)session);
                 }

             }
         }

    }

}


static void* test_cluster_do(Cluster* cluster){

}


static int submit_with_session(Cluster* cluster, void(*function)(void*), void* session_p){
    
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

    push_back(&cluster->job_queue, cluster->job_queue.has_job , new_job);
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
    return session;
}


void task_fn(void* args){
    /*
     task fn
     - create file fd
     - find file from server
     - send file to client 

     */

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


static int job_scheduler_init(JobScheduler* scheduler, int num_lock){
    /*
    
     */
    scheduler->len = 0;
    scheduler->num_lock = num_lock;

    scheduler -> has_job = (struct BinSemaphore** )malloc(num_lock*sizeof(struct BinSemaphore* ));;

    for (int i=0; i< num_lock; i++){
        semaphore_init(scheduler, &scheduler->has_job[i]); 
        
    }

    return 0;
}


static int semaphore_init(JobScheduler* scheduler, struct BinSemaphore** node){
    *node = (struct BinSemaphore* )malloc(sizeof(struct BinSemaphore ));
    bsem_init((*node), 0);


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
    (*thread) -> semaphore_id = id;
    pthread_create(&(*thread)->pthread, NULL, (void * (*)(void* )) worker_do_passive, (*thread));
    pthread_detach((*thread)->pthread);

    return 0;
}


static int epoll_init(Cluster* cluster, int server_fd){
    
    return 0;

}

static int async_stream_init(Cluster** cluster_p){
    printf("Async stream init \n");
    Cluster* cluster = (*cluster_p);
    cluster->port = cluster->control->port;
    int opt = 1;
    int server_fd;
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd <0){
        perror("stream_init() scoket create failed\n");
        return -1;
    } 

    int flags = fcntl(server_fd, F_GETFL);
    flags |= O_NONBLOCK;
    if (fcntl(server_fd, F_SETFL, flags)<0){
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

/*
static int _stream_init(Cluster** cluster_p){
    Cluster* cluster = (*cluster_p);
    cluster->port = 32209;
    int opt = 6;
    int server_fd;
    
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd <0){
        perror("stream_init() socket create failed \n");
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
*/

struct File* find_file_only(char* file_name){
    File* file;
    char file_path[2048] = {0};

    char* path = "./server/book_file/";
    snprintf(file_path, strlen(path)+1, "%s", path);
    strcat(file_path, file_name);
    
    FILE* fptr;
    fptr = fopen(file_path, "r");
    if (fptr==NULL){
        return NULL;
    } 
    file = (struct File* )malloc(sizeof(struct File));

    fread((file->contents_b), 1, sizeof(file->contents_b), fptr);

    
    snprintf(file->name,50, "%s", file_name);
    file->find = 1;
    fclose(fptr);
    return file;
}

/*
struct File* find_file(char* file_name){
    

    File* file;

    char file_path[1024] = {0};
    char* hard_path = "./server/book_file/";
    snprintf(file_path, strlen(hard_path)+1,"%s", hard_path);
    file = (struct File*)malloc(sizeof(struct File));
    DIR* os_fd;
    struct dirent* connector;
    os_fd = opendir(hard_path);

    if (os_fd==NULL){
        err("dir does not exists\n");
        free(file);
        return NULL;
    }


    while ((connector = readdir(os_fd))!= NULL){
        char* d_name = connector->d_name;
        if (strlen(d_name) ==strlen(file_name)){
            int same=1;
            for (int i =0; i< strlen(d_name); i++){
                if (d_name[i]!= file_name[i]){
                    same = 0;
                    break;
                }
            }
            if (same){
                // write
                strcat(file_path, d_name);
                
                FILE* fptr;
                fptr = fopen(file_path, "r");
                
                if(fptr==NULL){
                    printf("\t at find file():: fptr error check file path %s \n", file_path);
                    break;
                }
              
                fread((file->contents_b), 1, sizeof(file->contents_b), fptr);

                snprintf(file->name,50, "%s", d_name);
                file->find = 1;
                fclose(fptr);
                return file;
            }
        }
    }

    
    free(file);
    return NULL;
}


*/



struct Cluster* cluster_init(Control* control){
    /*
    Allocate object to memory
    First create all the object at the process
    If all works done, give them to thread

    1) create cluster
    2) create job-queue
    3) create stream
    4) create worker -> not a terminate condition
    
     
     */

    Cluster* cluster;
    cluster = (struct Cluster* )malloc(sizeof(struct Cluster ));
    if (cluster==NULL){
        err("cluster_init():: allocatate cluster on memory failed\n");
        return NULL;
    } 
    
    cluster->control = control;
    cluster->num_worker = control->num_worker;
    int num_worker = control->num_worker;
    /*create job queue, terminate condition */
    if (job_queue_init(&cluster->job_queue) < 0){
        err("could not allocate job queue ");
        free(cluster);
        return NULL;
    }
    if (worker_queue_init(cluster,&cluster->worker_queue, num_worker)<0){
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
    /*
    if (_stream_init(&cluster)<0){
        err("|_create stream Failed\n");
        return NULL;
    }
    */
    /*create lock*/ 
    pthread_mutex_init(&(cluster->lock), NULL);
    pthread_cond_init(&cluster->idle, NULL);
    /*create worker thread */
    for (int i=0; i<num_worker; i++){
        worker_init(cluster, &cluster->workers[i], i );
        printf("cluster init : Work thread %d created\n", i);
    }

     
    return cluster;
}


static int control_init(Control* control){
    /*
     
     I/O from server/control.txt
     */
    FILE* fptr;
    // hard coding --> add  argparse later version
    // char* root = "./server/control.txt";
    char* root = "./server/control.private.txt";
    
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
        
    }
    
    return 0;

}


int main(){
    Control* control;
    if ( control_init(control) <0){
        return -1;
    }
    SERVICE_KEEPALIVE =1;

    Cluster* cluster = cluster_init(control);
    
    pthread_create(&(cluster->pipe_thread), NULL, (void * (*)(void* )) pipeline, cluster);
    pthread_create(&(cluster->schedule_thread), NULL, (void * (*)(void* )) scheduler_do, cluster);
    pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) cluster_do, cluster);

    pthread_detach(cluster->main_thread);
    pthread_detach(cluster->pipe_thread);
    pthread_detach(cluster->schedule_thread);
    while ( SERVICE_KEEPALIVE){
        continue;

        }
    printf("----Service Stop----");
    return 0;
}
