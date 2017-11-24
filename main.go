package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/optiopay/kafka"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	startDate    = flag.Int("s", 0, "set start date")
	endDate      = flag.Int("e", 0, "set end date")
	specifyDate  = flag.Int("d", 0, "set one day, higher priorty then start/end")
	topic        = flag.String("t", "", "set topic")
	configFile   = flag.String("c", "./kfk.yml", "set config file")
	outputFile   = flag.String("o", "-", "output file, '-' is output in stdout")
	perfileLines = flag.Int64("l", 0, "split files when reach lines,0=no split")
    outputFormat = flag.String("f", "text", "output format, available: text, csv")
	outputFields = flag.String("fields", "", "output fields limit, payload must be json string format")
	batchSize    = flag.Int("bs", 10000, "batch write size")
)

type OutputHandle struct {
	Topic      string
	File       *os.File
	WriteLines int64
	Seq        int
	Mutex      *sync.Mutex
	Writer     *csv.Writer
}

type Payload map[string]interface{}

type Config struct {
	Servers     map[string][]string          `yaml:"servers"`
	TopicFields map[string]map[string]string `yaml:"topic_fields"`
}

var oh *OutputHandle
var wg sync.WaitGroup
var topicFields []string

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	oh = &OutputHandle{
		Topic: *topic,
		File:  os.Stdout,
		Mutex: new(sync.Mutex),
	}
	var err error
	if *outputFile != "-" && *outputFile!="" {
		oh.File, err = os.OpenFile(*outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
		if err != nil {
			fmt.Println(err)
			os.Exit(2)
		}
		defer oh.File.Close()
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
	if *outputFormat == "csv" {
		var tf map[string]string
		var ok bool
		if tf, ok = cfg.TopicFields[*topic]; !ok {
			fmt.Println("Topic not found in field configuration", *topic)
			os.Exit(1)
		}
		oh.Writer = csv.NewWriter(oh.File)
		var ofs []string
		if *outputFields != "" && *outputFields != "-" {
			ofs = strings.Split(*outputFields, ",")
		}
		for f, _ := range tf {
            if len(ofs)>0 {
                for _, of := range ofs {
                    if of == f {
                        topicFields = append(topicFields, f)
                    }
                }
            } else {
                topicFields = append(topicFields, f)
            }
        }
		oh.Writer.Write(topicFields)
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
				var toWrite []string
				var toWriteCsv [][]string
				var i int
				var payload Payload
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
					msg.Value = bytes.Replace(msg.Value, []byte("\n"), []byte("<br>"), -1)
					if *outputFormat == "csv" {
						if err = json.Unmarshal(msg.Value, &payload); err != nil {
							fmt.Println(err)
							continue
						}
						var row []string
						var isSubField bool
						var subFieldName string
						for _, field := range topicFields {
							isSubField = false
							if strings.Contains(field, ".") {
								subFields := strings.Split(field, ".")
								subFieldName = subFields[len(subFields)-1]
								field = subFields[0]
								isSubField = true
							}
							if v, existed := payload[field]; existed {
								switch v.(type) {
								case int:
									row = append(row, strconv.Itoa(v.(int)))
								case int64:
									row = append(row, fmt.Sprintf("%d", v.(int64)))
								case float32:
									row = append(row, fmt.Sprintf("%f", v.(float32)))
								case float64:
									row = append(row, fmt.Sprintf("%d", uint64(v.(float64))))
								case string:
									if isSubField {
										fmt.Printf("v.string= %s\n", v.(string))
										preg := regexp.MustCompile(subFieldName + `\s*=[\s"]*(\d+)["\s]*`)
										sm := preg.FindAllString(v.(string), 1)
										if len(sm) > 1 {
											row = append(row, sm[1])
										} else {
											row = append(row, "0")
										}
									} else {
										row = append(row, v.(string))
									}
								case map[string]interface{}:
									//fmt.Printf("MAP: %#v\n", v)
									newMap := v.(map[string]interface{})
									if nv, ok := newMap[subFieldName]; ok {
										switch nv.(type) {
										case int:
											row = append(row, fmt.Sprintf("%f", nv.(int)))
										case int64:
											row = append(row, fmt.Sprintf("%f", nv.(int64)))
										case float32:
											row = append(row, fmt.Sprintf("%f", nv.(float32)))
										case float64:
											row = append(row, fmt.Sprintf("%d", uint64(nv.(float64))))
										case string:
											row = append(row, v.(string))
										default:
											row = append(row, fmt.Sprintf("%v", nv))
										}
									} else {
										row = append(row, "")
									}
								default:
									fmt.Printf("D: %T\n", v)
									x, err := json.Marshal(v)
									if err != nil {
										fmt.Println(err)
									}
									row = append(row, string(x))
								}
							} else {
								//fmt.Printf("Not found: field=%s,value=%#v\n", field, v)
								row = append(row, "")
							}
						}
						toWriteCsv = append(toWriteCsv, row)
					} else {
						toWrite = append(toWrite, string(msg.Value))
					}
					if i > *batchSize {
						oh.Mutex.Lock()
						if *outputFormat == "csv" {
							oh.Writer.WriteAll(toWriteCsv)
							toWriteCsv = [][]string{}
						} else {
							oh.File.WriteString(strings.Join(toWrite, "\n"))
							toWrite = []string{}
						}
						oh.WriteLines += int64(i)
						i = 0
						if *perfileLines > 0 {
							if oh.WriteLines > *perfileLines {
								oh.File.Close()
								oh.File, err = os.OpenFile(*outputFile+"."+strconv.Itoa(oh.Seq), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0755)
								if err != nil {
									fmt.Println(err)
									break
								}
								if *outputFormat == "csv" {
									oh.Writer = csv.NewWriter(oh.File)
									oh.Writer.Write(topicFields)
								}
								oh.WriteLines = 0
								oh.Seq += 1
							}
						}
						oh.Mutex.Unlock()
					}
					i++
				}
				if i > 0 {
					oh.Mutex.Lock()
					if *outputFormat == "csv" {
						oh.Writer.WriteAll(toWriteCsv)
					} else {
						oh.File.WriteString(strings.Join(toWrite, "\n"))
					}
					toWrite = []string{}
					if *perfileLines > 0 {
						oh.WriteLines += int64(i)
					}
					oh.Mutex.Unlock()
				}
				fmt.Printf("Topic [%s] partition [%d] done\n", topic, partition)
			}(consumer, cc.Topic, i)
		}
	}
}

