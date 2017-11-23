GO=/opt/go/bin/go

all: clean prepare
	${GO} build -ldflags "-w -s" -o kfk

prepare:
	${GO} get -v github.com/optiopay/kafka
	${GO} get -v gopkg.in/yaml.v2

clean:
	rm -f kfk
