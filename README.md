
Testing word count application
    
    [new terminal]
    1. cd main
    2. go build -buildmode=plugin ../mrapps/wc.go && go run mrmaster.go pg-*.txt
    
    [another terminal]
    1. cd main
    2. ``````go run mrworker.go wc.so``````