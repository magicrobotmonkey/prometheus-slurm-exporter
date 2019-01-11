/* Copyright 2017 Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"os/exec"
	"strings"
	"strconv"
)


type QueueInfo map[string]QueueCounts
type QueueCounts map[string]float64
type QueueMetrics struct {
	slurm_queue QueueInfo
}

// Returns the scheduler metrics
func QueueGetMetrics() *QueueMetrics {
	return ParseQueueMetrics(QueueData())
}

func ParseQueueMetrics(input []byte) *QueueMetrics {
	qm := QueueMetrics{}
	qm.slurm_queue = make(map[string]QueueCounts)

	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if strings.Contains(line, ",") {
			splitted := strings.Split(line, ",")

			partition := splitted[0]
			user := splitted[1]
			name := splitted[2]
			state := splitted[3]

			cores, err := strconv.ParseFloat(splitted[4],64)
			if err != nil { log.Fatal(err) }
			mem, err := strconv.ParseFloat(splitted[5],64)
			if err != nil { log.Fatal(err) }

			indexstr := strings.Join([]string{partition, user, name, state}, ",")

			if qm.slurm_queue[indexstr] == nil {
				qm.slurm_queue[indexstr] = make(map[string]float64)
			}

			qm.slurm_queue[indexstr]["jobs"]++
			qm.slurm_queue[indexstr]["cores"]+=cores
			qm.slurm_queue[indexstr]["mem"]+=mem
		}
	}
	return &qm
}

// Execute the squeue command and return its output
func QueueData() []byte {
	cmd := exec.Command("squeue", "-h", "-o '%P,%u,%j,%T,%C,%m", "--states=all")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm queue metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewQueueCollector() *QueueCollector {
	labels := []string{"partition", "user", "name", "state"}
	return &QueueCollector{
		slurm_queue_jobs:     prometheus.NewDesc("slurm_queue_jobs", "Pending jobs in queue", labels, nil),
	}
}

type QueueCollector struct {
	slurm_queue_jobs     *prometheus.Desc
}

func (qc *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- qc.slurm_queue_jobs
}

func (qc *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	qm := QueueGetMetrics()
	if len(qm.slurm_queue) > 0 {
		for index, job_info := range  qm.slurm_queue {
			splitted := strings.Split(index, ",")
			partition := splitted[0]
			user := splitted[1]
			name := splitted[2]
			state := splitted[3]

			ch <- prometheus.MustNewConstMetric(
				qc.slurm_queue_jobs,
				prometheus.GaugeValue,
				job_info["jobs"],
				partition, 
				user, 
				name,
				state,
			)
		}
	}
}
