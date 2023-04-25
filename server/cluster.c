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
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#define err(str) fprintf(stderr, str)

static volatile int SERVICE_KEEPALIVE;



/*----------class-----------*/
typedef struct Control{
    long start_time;
    int open_port; 
    int inet;
} Control;


typedef struct HashItem{
    char* key;
    char* value;

} HashItem;

typedef struct FileHash{
    HashItem** items;
    int size;
    int len;


} FileHash;

typedef struct Session{
    pthread_mutex_t timed_t;
    uintptr_t session_iid;
    int worker_id;
    struct timespec* timeout;
    char read_buffer[1024];
    int varlead_status;
    int time_out_default;
    
} Session;

typedef struct BinSemaphore{
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int v;
} BinSemaphore;

typedef struct Job{
    struct Job* prev;
    void (*function )(void* args);
    void* args;
    Session* info;
} Job;

typedef struct JobQueue{
    pthread_mutex_t rmutex;
    Job* front;
    Job* rear;

    int len;
    BinSemaphore* has_job;

} JobQueue;


typedef struct Worker{
    pthread_t pthread;
    int id;

    struct Cluster* cluster;

    /*job*/
    int session;

} Worker;


typedef struct Cluster{
    pthread_t main_thread;
    pthread_mutex_t lock;
    pthread_cond_t idle;

    Worker** workers;
    int num_worker;
    int num_alive_worker;
    int num_working_worker;
    JobQueue job_queue;

    int server_fd;
    struct sockaddr_in serv_addr;
    int port;
    char inet[16];
    int connection;


} Cluster;


/*--------------------------*/

/*-----------call------------*/
struct Cluster;
struct Worker;

static int worker_init(Cluster* cluster, struct Worker** thread_p, int id);

static int submit(Cluster* cluster_p, void(*function_p)(void* ), void* args_p );

void task_fn(void* args);
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


static void time_wait(BinSemaphore* bsem_p){
    // not used, aren't fit at job -shedule function
    pthread_mutex_lock(&bsem_p->mutex);
    while (bsem_p->v != 1){
        continue;
//        pthread_cond_timedwait(&bsem_p->cond, &bsem_p->mutex, 30);
    }
    printf("time out,");
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
    pthread_mutex_init(&(job_queue_p->rmutex), NULL);
    bsem_init(job_queue_p->has_job, 0);
    

    return 0;
}

static void push(JobQueue* job_queue_p, struct Job* new_job){

    pthread_mutex_lock(&job_queue_p->rmutex);
    new_job->prev = NULL;

    if (job_queue_p->len==0){
        job_queue_p->front = new_job;
        job_queue_p->rear = new_job;
    }
    if (job_queue_p->len==1){
        new_job->prev = job_queue_p->front;
        job_queue_p->rear = job_queue_p ->front;
        job_queue_p-> front = new_job;

    }
    if (job_queue_p->len>=2){
        new_job->prev = job_queue_p->front;
        job_queue_p->front = new_job;
    }
    job_queue_p -> len ++ ;
    bsem_post(job_queue_p->has_job);
    pthread_mutex_unlock(&job_queue_p->rmutex);
    
}

static struct Job*  pop(JobQueue* job_queue_p){
    pthread_mutex_lock(&job_queue_p->rmutex);
    
    Job* front_job = job_queue_p->front;

    if (job_queue_p->len==0){
        


    }
    else if (job_queue_p -> len==1){
        job_queue_p->front = NULL;
        job_queue_p->rear = NULL;
        job_queue_p -> len = 0;
    }
    else if (job_queue_p->len >=2){
        job_queue_p->front = front_job->prev;
        job_queue_p->len --;
        bsem_post(job_queue_p->has_job);

    }
    
    
    pthread_mutex_unlock(&job_queue_p->rmutex);

    return front_job;
}


static void* worker_do(Worker* thread_p){
    char worker_name[16] = {0};
    snprintf(worker_name, 16, "worker#%d", thread_p->id);
    Cluster* cluster_p = thread_p->cluster;
    
    pthread_mutex_lock(&cluster_p->lock);
    cluster_p->num_alive_worker +=1;
    pthread_mutex_unlock(&cluster_p->lock);

    while (SERVICE_KEEPALIVE) {
        wait(cluster_p->job_queue.has_job);
        if (SERVICE_KEEPALIVE){

            pthread_mutex_lock(&cluster_p->lock);
            cluster_p->num_working_worker +=1;
            pthread_mutex_unlock(&cluster_p->lock);



            void(* fn_buf)(void* );
            void* args_buf;
            Job* job_p = pop(&cluster_p->job_queue);
            if (job_p){
                fn_buf = job_p->function;
                //args_buf = job_p->args;
                args_buf = job_p->info;
                fn_buf(args_buf);
                
                
                free(job_p->info);
                free(job_p);
                printf("--at work thread -- job done\n");
            }
            /*temporerly give latnecy*/
            printf("sleep - latnecy::thraed[%s]  \n", worker_name);
            sleep(10);
            printf("wake up- latnecy::thraed[%s] \n", worker_name);
            /*delete later */
        }
        
        pthread_mutex_lock(&cluster_p->lock);
        cluster_p->num_working_worker -= 1;
        pthread_mutex_unlock(&cluster_p->lock);
    
    }

    pthread_mutex_lock(&cluster_p->lock);
    cluster_p->num_alive_worker -= 1;
    pthread_mutex_unlock(&cluster_p->lock);
    return NULL;
    
}



static void* test_cluster_do(Cluster* cluster){

    int new_session;
    int socklen = sizeof(cluster->serv_addr);
    printf("TEST:: cluster test connections statsus %d \n", cluster->connection);
    printf("--debug-cluster do : fd %d\n ", cluster->server_fd);
//    new_session = accept(cluster->server_fd, (struct sockaddr* )&(cluster->serv_addr), (socklen_t* )&(cluster->serv_addr));
    new_session = accept(cluster->server_fd, (struct sockaddr* )&(cluster->serv_addr), &socklen);
    printf("--debug-cluster-new_session: %d\n", new_session); 
    
    int valread;
    char file_context[1024] = {0};
    snprintf(file_context,1024 ,"./tmp/loc");
    char buffer[1024] = {0};

    valread = read(new_session, buffer, 1024);
    printf("recved from client ::: %s\n", buffer);
    send(new_session, file_context, strlen(file_context), 0);
    printf("Hello message sent\n");

}

static void* cluster_do(Cluster* cluster){
    int new_session;
    int session_len = sizeof(cluster->serv_addr);

    printf("Cluster do connection status(%d)\n", cluster->connection);
    while (cluster->connection ){
        /* create session */
        new_session = accept(cluster->server_fd, (struct sockaddr* )&(cluster->serv_addr), &session_len);
        if (new_session <0){
            err("cluster- Accept error found\n");
        }

        /*create and submit session to job queue */
        submit(cluster, task_fn, (void* )(uintptr_t)new_session);
        
        printf("new session created {%d}\n", new_session);


    }
    
}

static int submit(Cluster* cluster_p, void(*function_p)(void* ), void* args_p ){
    /*mapping session and schedule at job queue */
    Job* new_job;
 
    
    new_job = (struct Job* )malloc(sizeof(struct Job));
    if (new_job==NULL){
        err("at submit():: could not allocate memory for new job\n");
        return -1;
    }
         
    /*map session */
    Session* session_info;
    session_info = (struct Session* )malloc(sizeof(struct Session));
    if (session_info ==NULL){
    
        err("at submit():: could not allocate memory for new session\n");
        return -1;
    }
    session_info->session_iid = (uintptr_t) args_p;
    session_info->varlead_status;
    
    pthread_mutex_init(&(session_info->timed_t), NULL);

    session_info->timeout = (struct timespec* )malloc(sizeof(struct timespec ));
    if (session_info->timeout==NULL){
        err("at submit():: could not allocate memory for timeout");
        return -1;
    }
    /*--<#>---------default value hard coding------------------   */
    session_info->time_out_default = 90;


    /*process task at function */
     
    new_job->function = function_p;
    new_job->args = args_p ;
    new_job->info = session_info;
    
    push(&cluster_p->job_queue, new_job);
    return 0;

}

void task_fn(void* args){
    /*procceing session :: read find write send */
    int timeout;
    
    Session* session_p = (Session* )args;
    session_p->timeout->tv_sec = session_p->time_out_default; // default 90s
    
    printf("Thread #%u working on %ld, || timeout setting (%ld)\n", (int)pthread_self(), (uintptr_t) args, session_p->timeout->tv_sec);
//    timeout = pthread_mutex_timedlock(&(session_p->timed_t), session_p->timeout);
    timeout = 0;
    while (timeout==0){

 
        session_p->varlead_status = read(session_p->session_iid, session_p->read_buffer, 1024);
    
        printf("recved from client :: %s \n", session_p->read_buffer);
    /*find file*/
        char find_file_loc[1024] = {0};
    
        snprintf(find_file_loc, 1024, "./tmp/loc/a.txt");
        send(session_p->session_iid, find_file_loc, strlen(find_file_loc),0);
        printf("send msg to %ld \n", session_p->session_iid);
        timeout = 1;
    }
    printf("---session time out----\n");
    //close(session_p->session_iid);
}


static int worker_init(Cluster* cluster, struct Worker** thread_p, int id){
    *thread_p = (struct Worker*)malloc(sizeof(struct Worker));
    if (thread_p==NULL){
        err("worker_init():: allocate worker on memory failed");
        return -1;
    }

    (*thread_p) -> cluster = cluster;
    (*thread_p) -> id = id;
    
    pthread_create(&(*thread_p)->pthread, NULL, (void * (*)(void* )) worker_do, (*thread_p));
    pthread_detach((*thread_p)->pthread);

    return 0;
}



static int _stream_init(Cluster** cluster){
    Cluster* cluster_p = (*cluster);
    cluster_p->port = 32209;
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
    cluster_p->serv_addr.sin_family=AF_INET;
    cluster_p->serv_addr.sin_addr.s_addr = INADDR_ANY;
    cluster_p->serv_addr.sin_port = htons(cluster_p->port);
    if (bind(server_fd, (struct sockaddr*)&(cluster_p->serv_addr), sizeof((cluster_p->serv_addr)))<0){
        err("stream_init : Bind Failed\n");
        return -1;
    }


    if (listen(server_fd, 3)<0){
        perror("listen Failed\n");
        return -1;
    }

    printf("stream done\n");
    cluster_p -> server_fd = server_fd;
    cluster_p->connection = 1;
    return 0;
}



struct Cluster* cluster_init(int num_worker){

    Cluster* cluster;
    cluster = (struct Cluster* )malloc(sizeof(struct Cluster ));
    if (cluster==NULL){
        err("cluster_init():: allocatate cluster on memory failed\n");
        return NULL;
    } 
    
    /*create job queue */
    if (job_queue_init(&cluster->job_queue) < 0){
        err("could not allocate job queue ");
        /*free here */
        free(cluster);
        return NULL;
    }


    cluster->workers = (struct Worker** )malloc(num_worker * sizeof(struct Worker* ));
    if (cluster->workers==NULL){

        err("cluster_init():: allocatate worker  on memory failed\n");
        return NULL;
    }
    
    /*create stream on cluster*/
    if (_stream_init(&cluster)<0){
        err("|_create stream Failed\n");
        return NULL;
    }
    /*create lock*/ 
    pthread_mutex_init(&(cluster->lock), NULL);
    pthread_cond_init(&cluster->idle, NULL);
    /*create worker thread */
    for (int i=0; i<num_worker; i++){
        worker_init(cluster, &cluster->workers[i], i );
        printf("Debug on cluster init : Work thread %d created\n", i);
    }

     
    return cluster;
}



void flush(Cluster* cluster_p){
    /*worker thread*/
    for (int i=0; i < cluster_p->num_alive_worker; i++){

       free( cluster_p -> workers[i]);
    }


}

int main(){
    
    SERVICE_KEEPALIVE =1;
    Cluster* cluster = cluster_init(10);
    /*stream processing start here */
    pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) cluster_do, cluster);

    //pthread_create(&(cluster->main_thread), NULL, (void * (*)(void* )) test_cluster_do, cluster);

    pthread_detach(cluster->main_thread);

    while ( SERVICE_KEEPALIVE){
        // wait
        continue;

        }
    SERVICE_KEEPALIVE = 0;
    printf("----Service Stop----");
    return 0;
}
