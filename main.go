package main

import (
	"flag"
	"fmt"
	"github.com/optiopay/kafka"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	startDate    = flag.Int("s", 0, "set start date")
	endDate      = flag.Int("e", 0, "set end date")
	specifyDate  = flag.Int("d", 0, "set one day, higher priorty then start and end")
	topic        = flag.String("t", "", "set topic")
	configFile   = flag.String("c", "./kfk.yml", "set config file")
	outputFile   = flag.String("o", "-", "output file, '-' is output in stdout")
	perfileLines = flag.Int64("l", 0, "split files when reach lines,0=no split")
)

type OutputHandle struct {
	Topic      string
	File       *os.File
	UseFile    bool
	WriteLines int64
	Seq        int
	Mutex      *sync.Mutex
}
type Config struct {
	Servers     map[string][]string          `yaml:"servers"`
}

type Payload map[string]interface{}

var oh *OutputHandle
var wg sync.WaitGroup

func main() {
	flag.Parse()
	if *outputFile == "" {
		fmt.Println("Output file is empty")
		os.Exit(1)
	}
	oh = &OutputHandle{
		Topic: *topic,
		File:  os.Stdout,
		Mutex: new(sync.Mutex),
	}
	var err error
	if *outputFile != "" && *outputFile != "-" && *outputFile != "sqlite" {
		oh.File, err = os.OpenFile(*outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			fmt.Println(err)
			os.Exit(2)
		}
		defer oh.File.Close()
		oh.UseFile = true
	}

	cfg := Config{}
	cfgFile, err := ioutil.ReadFile(*configFile)
	err = yaml.UnmarshalStrict(cfgFile, &cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
	//fmt.Printf("%#v\n", cfg)
	kfkServer := []string{}
	for s, m := range cfg.Servers {
		for _, t := range m {
			if t == *topic {
				kfkServer = append(kfkServer, s)
			}
		}
	}
	if *specifyDate > 0 {
		startDate = specifyDate
		endDate = specifyDate
	}
	//fmt.Println("kfkServer=", kfkServer)
	if *startDate < 20170630 || *endDate < 20170630 {
		fmt.Println("Invalid start end date")
		return
	}
	t0, err := time.Parse("20060102", strconv.Itoa(*startDate))
	if err != nil {
		fmt.Println(err)
		return
	}
	t1, err := time.Parse("20060102", strconv.Itoa(*endDate))
	if err != nil {
		fmt.Println(err)
		return
	}
	if t0.After(t1) {
		fmt.Println("start date > end date")
		return
	}
	consume(kfkServer, *topic, t0, t1)
	wg.Wait()
}

func consume(server []string, topic string, startDate time.Time, endDate time.Time) {
	diffHours := (endDate.Unix()-startDate.Unix())/3600 + 5
	conf := kafka.NewBrokerConf(fmt.Sprintf("%s-%d-%d", topic, startDate, endDate))
	broker, err := kafka.Dial(server, conf)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer broker.Close()
	metaResp, err := broker.Metadata()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var partitionCount int
	for _, metaTopic := range metaResp.Topics {
		if strings.HasPrefix(metaTopic.Name, topic) {
			partitionCount = len(metaTopic.Partitions)
			break
		}
	}

	var t0 int64

	for t0 = 0; t0 <= diffHours; t0 = t0 + 24 {
		t1 := startDate.Add(time.Duration(t0) * time.Hour)
		var i int
		for i = 0; i < partitionCount; i++ {
			cc := kafka.NewConsumerConf(fmt.Sprintf("%s%s.stat", topic, t1.Format("20060102")), int32(i))
			cc.StartOffset = kafka.StartOffsetOldest
			cc.RetryLimit = 1
			consumer, err := broker.Consumer(cc)
			if err != nil {
				fmt.Println(err)
				continue
			}
			wg.Add(1)
			go func(consumer kafka.Consumer, topic string, partition int) {
				defer wg.Done()
				var toWrite []byte
				var i int
				for {
					msg, err := consumer.Consume()
					if err != nil {
						if err == kafka.ErrNoData {
							//fmt.Println("No data")
							break
						}
						fmt.Println("error: ", err)
						break
					}
					toWrite = append(toWrite, msg.Value...)
					toWrite = append(toWrite, '\n')
					if i > 5000 {
						oh.File.Write(toWrite)
						oh.WriteLines += int64(i)
						toWrite = []byte{}
						i = 0
						if *perfileLines > 0 {
							if oh.WriteLines > *perfileLines {
								oh.File.Close()
								oh.File, err = os.OpenFile(*outputFile+"."+strconv.Itoa(oh.Seq), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
								if err != nil {
									fmt.Println(err)
									break
								}
								oh.WriteLines = 0
								oh.Seq += 1
							}
						}
					}
					i++
				}
				if i > 0 {
					oh.File.Write(toWrite)
					toWrite = []byte{}
					if *perfileLines > 0 {
						oh.WriteLines += int64(i)
					}
				}
				fmt.Printf("Topic [%s] partition [%d] done\n", topic, partition)
			}(consumer, cc.Topic, i)
		}
	}
}
