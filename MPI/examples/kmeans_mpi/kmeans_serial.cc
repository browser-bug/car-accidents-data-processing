#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <math.h>
#include <iostream>
#include <iomanip>
#include <random>

using namespace std;

double distance(double x1, double y1, double x2, double y2) ;
void initial_placement(double *points_x, double *points_y, int num_points, double *centroids_x, double *centroids_y, int num_centroids, int seed) ;
double cpuSecond();

int main(int argc, char **argv) {
  int num_points, num_clusters, num_iter, seed;
  double *points_x, *points_y, *centroids_x, *centroids_y;
  int *assignment; // for each point i assignment[i] is the cluster i was assigned to
  int *num_assigned; // for each cluster i num_assigned[i] is the number of points assigned to cluster i
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
  points_x = new double[num_points];
  points_y = new double[num_points];
  centroids_x = new double[num_clusters];
  centroids_y = new double[num_clusters];
  assignment = new int[num_points];
  num_assigned = new int[num_clusters];
  
  // calculate initial placement (random)
  initial_placement(points_x, points_y, num_points, centroids_x, centroids_y, num_clusters, seed);

  cout << fixed << setprecision(8);
  
  double begin = cpuSecond();

  // iterate
  for(int i=0; i<num_iter; i++) {
    cout << "Iteration " << i << "\t: ";

    // clear data structures
    for(int c=0; c<num_clusters; c++) num_assigned[c]=0;

    // assign points to clusters
    for(int p=0; p<num_points; p++) {
      best=0;
      min_dist=10.0;
      for(int c=0; c<num_clusters; c++) {
        dist = distance(points_x[p], points_y[p], centroids_x[c], centroids_y[c]);
        if(dist < min_dist) {
          best=c;
          min_dist = dist;
        }
      }
      assignment[p] = best;
      num_assigned[best]++;
    }

    // clear old centroids
    for(int c=0; c<num_clusters; c++) {
      centroids_x[c] = 0.0;
      centroids_y[c] = 0.0;
    }
    // calculate new centroids
    for(int p=0; p<num_points; p++) {
      centroids_x[assignment[p]] += points_x[p];
      centroids_y[assignment[p]] += points_y[p];
    }
    for(int c=0; c<num_clusters; c++) {
      centroids_x[c] = centroids_x[c] / num_assigned[c];
      centroids_y[c] = centroids_y[c] / num_assigned[c];
    }

    // calculate and print total distance
    tot_dist = 0.0;
    for(int p=0; p<num_points; p++) {
      tot_dist += distance(points_x[p], points_y[p], centroids_x[assignment[p]], centroids_y[assignment[p]]);
    }
    cout << "\tdistance is " << tot_dist << endl;
  }

  double duration = cpuSecond()-begin;
  
  cout << "It took " << duration << "s to run the algorithm" << endl;

  delete[] points_x;
  delete[] points_y;
  delete[] centroids_x;
  delete[] centroids_y;
  delete[] assignment;
  delete[] num_assigned;
}

double distance(double x1, double y1, double x2, double y2) {
  return sqrt( (x1-x2)*(x1-x2)+(y1-y2)*(y1-y2) );
}

void initial_placement(double *points_x, double *points_y, int num_points, double *centroids_x, double *centroids_y, int num_centroids, int seed) {
  uniform_real_distribution<> dis(-1.0, 1.0);
  mt19937 gen(seed);
  
  for (int i = 0; i < num_points; i++) {
    points_x[i] = dis(gen);
    points_y[i] = dis(gen);
  }
  
  for (int i = 0; i < num_centroids; i++) {
    centroids_x[i] = points_x[i];
    centroids_y[i] = points_y[i];
  }
}

double cpuSecond(){
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double)tp.tv_sec+(double)tp.tv_usec*1.e-6);
}
