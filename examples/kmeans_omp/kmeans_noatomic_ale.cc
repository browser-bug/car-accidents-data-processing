#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>
#include <iostream>
#include <iomanip>
#include <random>

#define NUM_THREADS 8

using namespace std;

class Point {
  public:
  double x;
  double y;
  Point(double x, double y) : x(x), y(y) {}
  Point() : x(0), y(0) {}
  double distance(Point &other);
};

inline double Point::distance(Point &other) {
  return sqrt( (x-other.x)*(x-other.x)+(y-other.y)*(y-other.y) );
}

void initial_placement(Point *points, int num_points, Point *centroids, int num_centroids, int seed) ;
double cpuSecond();

int main(int argc, char **argv) {
  int num_points, num_clusters, num_iter, seed;
  Point *points, *centroids;
  int *assignment; // for each point i assignment[i] is the cluster i was assigned to
  int *num_assigned; // for each cluster i num_assigned[i] is the number of points assigned to cluster i
  int *num_assigned_thread[NUM_THREADS];
  Point *centroids_thread[NUM_THREADS];
  double tot_dist;
  int best;
  double min_dist, dist;
  
  // parse arguments
  if (argc != 5) {
    cout << "Usage: " << argv[0] << " <num points> <num clusters> <num iterations> <seed>" << endl;
    exit(-1);
  }
  
  num_points = atoi(argv[1]);
  num_clusters = atoi(argv[2]);
  num_iter = atoi(argv[3]);
  seed = atoi(argv[4]);

  // allocate memory
  points = new Point[num_points];
  centroids = new Point[num_clusters];
  assignment = new int[num_points];
  num_assigned = new int[num_clusters];
  for (int i=0; i<NUM_THREADS; i++) {
    centroids_thread[i] = new Point[num_clusters];
    num_assigned_thread[i] = new int[num_clusters];
  }
  
  // calculate initial placement (random)
  initial_placement(points, num_points, centroids, num_clusters, seed);

  cout << fixed << setprecision(8);
  
  double begin = cpuSecond();

  // iterate
  for(int i=0; i<num_iter; i++) {
    cout << "Iteration " << i << "\t: ";

    // clear data structures
#pragma omp parallel for
    for(int c=0; c<num_clusters; c++) {
      num_assigned[c]=0;
      for (int t=0; t<NUM_THREADS; t++) {
	num_assigned_thread[t][c]=0;
	centroids_thread[t][c].x=0;
	centroids_thread[t][c].y=0;
      }
    }
    
    // assign points to clusters
#pragma omp parallel for private(best, dist, min_dist) schedule(dynamic, 100)
    for(int p=0; p<num_points; p++) {
      int t = omp_get_thread_num();
      best=0;
      min_dist=10.0;
      for(int c=0; c<num_clusters; c++) {
        dist = points[p].distance(centroids[c]);
        if(dist < min_dist) {
          best=c;
          min_dist = dist;
        }
      }
      assignment[p] = best;
      num_assigned_thread[t][best]++;
      centroids_thread[t][best].x += points[p].x;
      centroids_thread[t][best].y += points[p].y;
    }

    // calculate new centroids
#pragma omp parallel for
    for(int c=0; c<num_clusters; c++) {
      centroids[c].x = 0.0;
      centroids[c].y = 0.0;
      for (int t=0; t<NUM_THREADS; t++) {
	centroids[c].x += centroids_thread[t][c].x;
	centroids[c].y += centroids_thread[t][c].y;
	num_assigned[c] += num_assigned_thread[t][c];
      }
      centroids[c].x = centroids[c].x / num_assigned[c];
      centroids[c].y = centroids[c].y / num_assigned[c];
    }

    // calculate and print total distance
    tot_dist = 0.0;
#pragma omp parallel for reduction (+ : tot_dist)
    for(int p=0; p<num_points; p++) {
      tot_dist += points[p].distance(centroids[assignment[p]]);
    }
    cout << "\tdistance is " << tot_dist << endl;
  }

  double duration = cpuSecond()-begin;
  
  cout << "It took " << duration << "s to run the algorithm" << endl;
}

void initial_placement(Point *points, int num_points, Point *centroids, int num_centroids, int seed) {
  uniform_real_distribution<> dis(-1.0, 1.0);
  mt19937 gen(seed);
  
  for (int i = 0; i < num_points; i++) {
    points[i].x = dis(gen);
    points[i].y = dis(gen);
  }
  
  for (int i = 0; i < num_centroids; i++) {
    centroids[i].x = points[i].x;
    centroids[i].y = points[i].y;
  }
}

double cpuSecond(){
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double)tp.tv_sec+(double)tp.tv_usec*1.e-6);
}
