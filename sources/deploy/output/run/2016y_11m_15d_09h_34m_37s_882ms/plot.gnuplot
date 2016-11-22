reset
    set datafile separator ","
    set autoscale xfix
    set grid
    set ytics nomirror
    set autoscale y
    set tics out
    set y2tics
    set autoscale y2
plot \
    "plot.dat"    skip 1     using 1:2 with lines axes x1y1     title "data_02", \
    "plot.dat"    skip 1     using 1:3 with lines axes x1y1     title "data_04", \
    "plot.dat"    skip 1     using 1:4 with lines axes x1y2     title "data_05"

bind 'e' 'replot'
pause -1
