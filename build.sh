#/usr/bin/env sh

(cd Client && go build)
(cd Controller && go build)
(cd generator && go build)
(cd validator && go build)
(cd printer && go build)

