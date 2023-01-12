package pipeline_test

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/karthick18/pipeline"
	"github.com/stretchr/testify/assert"
)

type DagConfig struct {
	outputs []string
	inputs  []string
	//configure non-zero weight to set update precedence if there are multiple pipelines for same output
	weight uint
}

func testPipelineOp(t *testing.T, dagConfig map[string]DagConfig, pipelineInstance pipeline.Pipeline, pipelineName string) {
	expectedWaitListMap := map[string][]string{
		"FILE_IMPORTER":            []string{},
		"MAC_CAPTURE":              []string{"FILE_IMPORTER"},
		"DHCP":                     []string{"FILE_IMPORTER"},
		"VENDOR":                   []string{"FILE_IMPORTER"},
		"VENDOR2":                  []string{"FILE_IMPORTER"},
		"DNS":                      []string{"DHCP", "MAC_CAPTURE", "VENDOR", "VENDOR2", "FILE_IMPORTER"},
		"NESSUS":                   []string{"DNS", "DHCP", "MAC_CAPTURE", "VENDOR", "VENDOR2", "FILE_IMPORTER"},
		"NESSUS_HOST_NETWORK_SCAN": []string{"NESSUS", "DNS", "DHCP", "MAC_CAPTURE", "VENDOR", "VENDOR2", "FILE_IMPORTER"},
		"LEARNING_ML":              []string{"NESSUS_HOST_NETWORK_SCAN", "NESSUS", "DNS", "DHCP", "MAC_CAPTURE", "VENDOR", "VENDOR2", "FILE_IMPORTER"},
		"STOP":                     []string{"LEARNING_ML", "NESSUS_HOST_NETWORK_SCAN", "NESSUS", "DNS", "DHCP", "MAC_CAPTURE", "VENDOR", "VENDOR2", "FILE_IMPORTER"},
	}
	actualWaitListMap := make(map[string][]string)

	ops, err := pipelineInstance.PipelineOperations(pipelineName)
	assert.Nil(t, err, "error getting pipeline operations")

	channelMap := map[string]chan bool{}
	rand.Seed(time.Now().UnixNano())

	// simulate the runtime of the pipeline
	runCommand := func(name string) {
		defer func() {
			if ch, ok := channelMap[name]; ok {
				close(ch)
			}
		}()
		time.Sleep(time.Duration(rand.Intn(50)+1) * time.Millisecond)
		t.Log("RUN", name)
	}

	//simulate waiting on completion of dependent pipelines
	waitCommand := func(name string, waitPipelines []string) {
		// if we have channels, we wait on them to close
		for _, w := range waitPipelines {
			if _, ok := channelMap[w]; !ok {
				continue
			}
			//t.Log(name, "WAIT ON", w)
			// wait on them to close
			<-channelMap[w]
		}
	}
	//sanitize the pipelines to created pipelines (ignoring the fields)
	pipelineOps := []pipeline.PipelineOp{}
	for _, pipelineOp := range ops {
		if _, ok := dagConfig[pipelineOp.Name]; !ok {
			continue
		}
		waitPipelines := []string{}
		for _, waitPipeline := range pipelineOp.WaitList {
			if _, ok := dagConfig[waitPipeline]; ok {
				waitPipelines = append(waitPipelines, waitPipeline)
			}
		}
		pipelineOp.WaitList = waitPipelines
		pipelineOps = append(pipelineOps, pipelineOp)
		actualWaitListMap[pipelineOp.Name] = waitPipelines
		t.Log("Pipeline", pipelineOp.Name, "Waitlist", waitPipelines)
	}

	// create channels for all pipeline ops
	for _, pipelineOp := range pipelineOps {
		if _, ok := channelMap[pipelineOp.Name]; !ok {
			t.Log("Creating", pipelineOp.Name)
			channelMap[pipelineOp.Name] = make(chan bool, 1)
		}
	}
	for _, pipelineOp := range pipelineOps {
		if len(pipelineOp.WaitList) == 0 {
			go runCommand(pipelineOp.Name)
		} else {
			go func(pipelineOp pipeline.PipelineOp) {
				waitCommand(pipelineOp.Name, pipelineOp.WaitList)
				runCommand(pipelineOp.Name)
			}(pipelineOp)
		}
	}

	// wait for pipeline completion
	<-channelMap[pipelineName]

	for pipeline := range expectedWaitListMap {
		sort.Slice(expectedWaitListMap[pipeline], func(i, j int) bool {
			return expectedWaitListMap[pipeline][i] < expectedWaitListMap[pipeline][j]
		})
	}

	for pipeline := range actualWaitListMap {
		sort.Slice(actualWaitListMap[pipeline], func(i, j int) bool {
			return actualWaitListMap[pipeline][i] < actualWaitListMap[pipeline][j]
		})
	}

	for pipeline, waitList := range actualWaitListMap {
		expectedWaitList := expectedWaitListMap[pipeline]
		assert.Equal(t, reflect.DeepEqual(waitList, expectedWaitList), true, "Actual wait list does not match expected")
	}
}

func testPipeline(t *testing.T, dagConfig map[string]DagConfig, pipelineName string) {
	var pipelineConfig []pipeline.PipelineConfig
	for p, config := range dagConfig {
		pipelineConfig = append(pipelineConfig, pipeline.PipelineConfig{Name: p, Inputs: config.inputs, Outputs: config.outputs, Weight: config.weight})
	}
	pipelineInstance, err := pipeline.New(pipelineConfig)
	assert.Nil(t, err, "error creating pipeline")

	t.Log("Testing pipeline operations to stop pipeline", pipelineName)
	testPipelineOp(t, dagConfig, pipelineInstance, pipelineName)
}

func TestPipeline(t *testing.T) {
	dagConfig := map[string]DagConfig{}
	dagConfig["FILE_IMPORTER"] = DagConfig{outputs: []string{"mac", "port", "vlan", "vsid", "type"}, inputs: []string{}}
	dagConfig["DHCP"] = DagConfig{outputs: []string{"dhcp_ip", "dhcp_hostname"},
		inputs: []string{"mac"}}
	dagConfig["DNS"] = DagConfig{inputs: []string{"mac", "dhcp_ip"},
		outputs: []string{"dns_hostname"}}
	dagConfig["VENDOR"] = DagConfig{outputs: []string{"vendor"},
		inputs: []string{"mac"}}
	dagConfig["MAC_CAPTURE"] = DagConfig{outputs: []string{"mactable"},
		inputs: []string{"mac"}}
	dagConfig["NESSUS"] = DagConfig{outputs: []string{"NONE_CVE", "CRITICAL_CVE", "MEDIUM_CVE", "INFO_CVE"},
		inputs: []string{"dhcp_ip", "dhcp_hostname", "dns_hostname"}}
	dagConfig["NESSUS_HOST_NETWORK_SCAN"] = DagConfig{outputs: []string{"CRITICAL_NETWORK_RISK"},
		inputs: []string{"dhcp_ip", "CRITICAL_CVE"}}
	dagConfig["LEARNING_ML"] = DagConfig{outputs: []string{"riskscore"},
		inputs: []string{"mac", "dhcp_ip", "dhcp_hostname", "dns_hostname", "vendor", "vlan", "port", "mactable", "CRITICAL_NETWORK_RISK"}}
	dagConfig["STOP"] = DagConfig{outputs: []string{"db"},
		inputs: []string{"LEARNING_ML"}}
	// connect another node handling the same output with a higher weight
	dagConfig["VENDOR2"] = DagConfig{outputs: []string{"vendor"},
		inputs: []string{"mac"}, weight: 10}

	testPipeline(t, dagConfig, "STOP")
}
