# Need to define which nodes and adjust -t param
salloc -A rabl -p magic -w node-02,node-03 -t 0:5:0 --mem=0 --exclusive --no-shell