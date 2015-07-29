The masterReplicationHandler uses a Chan to ensure Update ordering. The handler
reading from the Chan does both, local updates and distributing via zmq.
Splitting the Chan should increase Performance with Salves connected quite a bit.

# before splitting chan

Benchmark Local: RUNNING...
benchmarking Local
time                 105.4 ms   (97.22 ms .. 115.2 ms)
                     0.990 R²   (0.979 R² .. 0.998 R²)
mean                 106.4 ms   (103.2 ms .. 111.0 ms)
std dev              5.919 ms   (3.762 ms .. 9.091 ms)
variance introduced by outliers: 10% (moderately inflated)

benchmarking Local-grouped
time                 7.285 ms   (7.040 ms .. 7.535 ms)
                     0.993 R²   (0.989 R² .. 0.996 R²)
mean                 6.108 ms   (5.830 ms .. 6.378 ms)
std dev              780.1 μs   (660.3 μs .. 906.1 μs)
variance introduced by outliers: 72% (severely inflated)

Benchmark Local: FINISH
Benchmark MasterOnly: RUNNING...
benchmarking MasterOnly
time                 111.9 ms   (105.0 ms .. 117.6 ms)
                     0.993 R²   (0.974 R² .. 0.999 R²)
mean                 107.7 ms   (102.9 ms .. 112.9 ms)
std dev              7.137 ms   (4.358 ms .. 10.91 ms)
variance introduced by outliers: 12% (moderately inflated)

benchmarking MasterOnly-grouped
time                 12.44 ms   (10.68 ms .. 16.04 ms)
                     0.763 R²   (0.637 R² .. 0.993 R²)
mean                 10.16 ms   (9.487 ms .. 12.36 ms)
std dev              2.992 ms   (1.021 ms .. 6.005 ms)
variance introduced by outliers: 93% (severely inflated)

Benchmark MasterOnly: FINISH
Benchmark MasterSlave: RUNNING...
benchmarking MasterSlave
time                 5.099 s    (4.994 s .. 5.181 s)
                     1.000 R²   (1.000 R² .. 1.000 R²)
mean                 5.102 s    (5.082 s .. 5.113 s)
std dev              17.90 ms   (0.0 s .. 19.14 ms)
variance introduced by outliers: 19% (moderately inflated)

benchmarking MasterSlave-grouped
time                 198.1 ms   (73.47 ms .. 400.3 ms)
                     0.704 R²   (0.237 R² .. 1.000 R²)
mean                 229.5 ms   (188.4 ms .. 306.5 ms)
std dev              64.98 ms   (2.340 ms .. 82.13 ms)
variance introduced by outliers: 65% (severely inflated)

# after splitting chan

benchmarking Local
time                 98.01 ms   (93.92 ms .. 104.8 ms)
                     0.995 R²   (0.989 R² .. 0.999 R²)
mean                 97.68 ms   (94.70 ms .. 100.3 ms)
std dev              4.134 ms   (2.843 ms .. 5.997 ms)

benchmarking Local-grouped
time                 7.386 ms   (7.093 ms .. 7.666 ms)
                     0.992 R²   (0.987 R² .. 0.996 R²)
mean                 6.105 ms   (5.806 ms .. 6.394 ms)
std dev              831.6 μs   (689.2 μs .. 1.003 ms)
variance introduced by outliers: 74% (severely inflated)

Benchmark Local: FINISH
Benchmark MasterOnly: RUNNING...
benchmarking MasterOnly
time                 106.5 ms   (98.30 ms .. 112.3 ms)
                     0.996 R²   (0.992 R² .. 0.999 R²)
mean                 99.60 ms   (97.03 ms .. 102.5 ms)
std dev              4.517 ms   (3.295 ms .. 6.592 ms)
variance introduced by outliers: 10% (moderately inflated)

benchmarking MasterOnly-grouped
time                 11.02 ms   (10.78 ms .. 11.33 ms)
                     0.996 R²   (0.993 R² .. 0.998 R²)
mean                 9.998 ms   (9.668 ms .. 10.28 ms)
std dev              841.6 μs   (679.3 μs .. 1.022 ms)
variance introduced by outliers: 45% (moderately inflated)

Benchmark MasterOnly: FINISH
Benchmark MasterSlave: RUNNING...
benchmarking MasterSlave
time                 129.6 ms   (124.8 ms .. 132.4 ms)
                     0.999 R²   (0.995 R² .. 1.000 R²)
mean                 126.4 ms   (124.4 ms .. 129.1 ms)
std dev              3.516 ms   (2.240 ms .. 5.186 ms)
variance introduced by outliers: 11% (moderately inflated)

benchmarking MasterSlave-grouped
time                 25.02 ms   (24.02 ms .. 26.07 ms)
                     0.989 R²   (0.978 R² .. 0.996 R²)
mean                 21.36 ms   (18.78 ms .. 22.72 ms)
std dev              4.140 ms   (1.663 ms .. 6.090 ms)
variance introduced by outliers: 74% (severely inflated)


