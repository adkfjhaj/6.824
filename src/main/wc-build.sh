go build -race -buildmode=plugin -gcflags="all=-N -l"  ../mrapps/wc.go

#rm -f mr-out*