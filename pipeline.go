package pipeline

import (
	"github.com/karthick18/go-graph/graph"
)

type PipelineConfig struct {
	Name    string
	Inputs  []string
	Outputs []string
	Weight  uint
}

type PipelineOp struct {
	Name     string
	WaitList []string
}

type Pipeline interface {
	PipelineOperations(name string) ([]PipelineOp, error)
}

type pipeline struct {
	dag                 graph.DAG
	outputMap           map[string][]string
	inputMap            map[string][]string
	weightMap           map[string]uint
	outputToPipelineMap map[string][]string
	inputToPipelineMap  map[string][]string
}

func New(config []PipelineConfig) (Pipeline, error) {
	p := &pipeline{dag: graph.NewDirectedGraph(),
		outputMap:           make(map[string][]string),
		inputMap:            make(map[string][]string),
		weightMap:           make(map[string]uint),
		outputToPipelineMap: make(map[string][]string),
		inputToPipelineMap:  make(map[string][]string),
	}

	for _, c := range config {
		p.setInputOutput(c)
	}

	if err := p.connectInputsToOutputs(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *pipeline) setInputOutput(config PipelineConfig) {
	p.outputMap[config.Name] = config.Outputs
	p.inputMap[config.Name] = config.Inputs
	p.weightMap[config.Name] = config.Weight

	for _, o := range config.Outputs {
		skip := false
		for _, n := range p.outputToPipelineMap[o] {
			if n == config.Name {
				skip = true
				break
			}
		}
		if !skip {
			p.outputToPipelineMap[o] = append(p.outputToPipelineMap[o], config.Name)
		}
	}

}

func (p *pipeline) connectInputsToOutputs() error {

	//resolve input pipelines
	for name, inputs := range p.inputMap {
		dependencies := make([]string, 0, len(inputs))
		for _, i := range inputs {
			if r, ok := p.outputToPipelineMap[i]; ok {
				dependencies = append(dependencies, r...)
				p.inputToPipelineMap[i] = append(p.inputToPipelineMap[i], r...)
			} else {
				// check in the outputMap to see if its a pipeline name
				if _, ok := p.outputMap[i]; ok {
					dependencies = append(dependencies, i)
					p.inputToPipelineMap[i] = append(p.inputToPipelineMap[i], i)
				}
			}
		}

		// connect all the dependencies to this pipeline
		switch {
		case len(dependencies) == 0:

			if err := p.dag.AddWithCost(graph.Edge{
				Node:     name,
				Neighbor: name,
				Cost:     p.weightMap[name],
			}); err != nil {
				return err
			}

		default:

			for _, dep := range dependencies {
				if err := p.dag.AddWithCost(graph.Edge{
					Node:     dep,
					Neighbor: name,
					Cost:     p.weightMap[dep],
				}); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (p *pipeline) PipelineOperations(operation string) ([]PipelineOp, error) {
	// do a topological sort for execution order
	nodes, err := p.dag.TopologicalSort()
	if err != nil {
		return nil, err
	}

	var pipelineOps []PipelineOp

	waitPipelines := make(map[int][]string, len(nodes))

	for index, n := range nodes {
		pipelineOps = append(pipelineOps, PipelineOp{Name: n.Node,
			WaitList: waitPipelines[n.Depth-1],
		})

		if n.Node == operation {
			break
		}

		waitPipelines[n.Depth] = append(waitPipelines[n.Depth], n.Node)
		if index < len(nodes)-1 && nodes[index+1].Depth != n.Depth {
			waitPipelines[n.Depth] = append(waitPipelines[n.Depth], waitPipelines[n.Depth-1]...)
		}
	}

	return pipelineOps, nil
}
