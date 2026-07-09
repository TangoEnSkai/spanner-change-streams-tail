//
// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cloudspannerecosystem/spanner-change-streams-tail/changestreams"
)

const (
	rootPartitionToken = "root"
)

type Partition struct {
	Token          string
	StartTimestamp time.Time
	EndTimestamp   time.Time
	RecordSequence string
	Parents        []*Partition
}

type MoveEvent struct {
	Source    string
	Dest      string
	Timestamp time.Time
}

type PartitionVisualizer struct {
	partitions map[string]*Partition
	timestamps map[time.Time]bool
	moves      []MoveEvent
	mu         sync.Mutex
	out        io.Writer
}

func NewPartitionVisualizer(out io.Writer) *PartitionVisualizer {
	partitions := make(map[string]*Partition)
	// Root partition.
	partitions[rootPartitionToken] = &Partition{Token: rootPartitionToken}
	return &PartitionVisualizer{
		partitions: partitions,
		timestamps: make(map[time.Time]bool),
		out:        out,
	}
}

func (v *PartitionVisualizer) Read(result *changestreams.ReadResult) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	hasImmutableKeyRange := false
	for _, r := range result.ChangeRecords {
		if len(r.ChildPartitionsRecords) > 0 {
			hasImmutableKeyRange = true
			break
		}
	}

	if hasImmutableKeyRange {
		return v.readImmutableKeyRange(result)
	}
	return v.readMutableKeyRange(result)
}

func (v *PartitionVisualizer) readImmutableKeyRange(result *changestreams.ReadResult) error {
	for _, changeRecord := range result.ChangeRecords {
		for _, partitionRecord := range changeRecord.ChildPartitionsRecords {
			for _, childPartition := range partitionRecord.ChildPartitions {
				token := childPartition.Token
				p := v.getOrCreatePartition(token)
				if !p.StartTimestamp.IsZero() {
					continue
				}
				p.StartTimestamp = partitionRecord.StartTimestamp
				p.RecordSequence = partitionRecord.RecordSequence

				var parents []*Partition
				for _, parentToken := range childPartition.ParentPartitionTokens {
					parent := v.getOrCreatePartition(parentToken)
					parents = append(parents, parent)
				}
				if len(parents) == 0 {
					parents = append(parents, v.partitions[rootPartitionToken])
				}
				p.Parents = parents
			}
		}
	}
	return nil
}

func (v *PartitionVisualizer) readMutableKeyRange(result *changestreams.ReadResult) error {
	currentPartitionToken := result.PartitionToken

	for _, changeRecord := range result.ChangeRecords {
		// Handle Partition Start
		for _, startRecord := range changeRecord.PartitionStartRecords {
			t := startRecord.StartTimestamp
			v.touchPartition(currentPartitionToken, t)
			for _, token := range startRecord.PartitionTokens {
				p := v.touchPartition(token, t)
				p.StartTimestamp = t
				p.RecordSequence = startRecord.RecordSequence
				if currentPartitionToken != "" {
					parent := v.getOrCreatePartition(currentPartitionToken)
					v.addParentLink(p, parent)
				}
			}
		}

		// Handle Partition End
		for _, endRecord := range changeRecord.PartitionEndRecords {
			t := endRecord.EndTimestamp
			token := endRecord.PartitionToken
			p := v.touchPartition(token, t)
			p.EndTimestamp = t
			p.RecordSequence = endRecord.RecordSequence
		}

		// Handle Partition Events
		for _, eventRecord := range changeRecord.PartitionEventRecords {
			t := eventRecord.CommitTimestamp
			token := eventRecord.PartitionToken
			v.touchPartition(token, t)

			for _, moveIn := range eventRecord.MoveInEvents {
				v.touchPartition(moveIn.SourcePartitionToken, t)
				v.addMove(moveIn.SourcePartitionToken, token, t)
			}

			for _, moveOut := range eventRecord.MoveOutEvents {
				v.touchPartition(moveOut.DestinationPartitionToken, t)
				v.addMove(token, moveOut.DestinationPartitionToken, t)
			}
		}
	}
	return nil
}

func (v *PartitionVisualizer) getOrCreatePartition(token string) *Partition {
	p, ok := v.partitions[token]
	if !ok {
		p = &Partition{Token: token}
		v.partitions[token] = p
	}
	return p
}

func (v *PartitionVisualizer) touchPartition(token string, t time.Time) *Partition {
	if token == "" {
		return nil
	}
	v.timestamps[t] = true
	p := v.getOrCreatePartition(token)
	if p.StartTimestamp.IsZero() || t.Before(p.StartTimestamp) {
		p.StartTimestamp = t
	}
	return p
}

func (v *PartitionVisualizer) addParentLink(child, parent *Partition) {
	for _, p := range child.Parents {
		if p.Token == parent.Token {
			return
		}
	}
	child.Parents = append(child.Parents, parent)
}

func (v *PartitionVisualizer) addMove(source, dest string, t time.Time) {
	for _, m := range v.moves {
		if m.Source == source && m.Dest == dest && m.Timestamp.Equal(t) {
			return
		}
	}
	v.moves = append(v.moves, MoveEvent{Source: source, Dest: dest, Timestamp: t})
}

func (v *PartitionVisualizer) Draw() {
	fmt.Fprintf(v.out, "digraph {\n")
	partitions := sortPartitions(v.partitions)

	if len(v.timestamps) == 0 {
		v.drawImmutableKeyRange(partitions)
	} else {
		v.drawMutableKeyRange(partitions)
	}
	fmt.Fprintf(v.out, "}\n")
}

func (v *PartitionVisualizer) drawImmutableKeyRange(partitions []*Partition) {
	fmt.Fprintf(v.out, "  node [shape=record];\n")
	for _, partition := range partitions {
		var timestamp string
		if !partition.StartTimestamp.IsZero() {
			timestamp = partition.StartTimestamp.Format(time.RFC3339)
		}
		fmt.Fprintf(v.out, `  "%s" [label="{token|start_timestamp|record_sequence}|{{%s}|{%s}|{%s}}"];`, partition.Token, partition.Token, timestamp, partition.RecordSequence)
		fmt.Fprintln(v.out, "")
	}
	for _, partition := range partitions {
		for _, parent := range partition.Parents {
			fmt.Fprintf(v.out, `  "%s" -> "%s"`, parent.Token, partition.Token)
			fmt.Fprintln(v.out, "")
		}
	}
}

func (v *PartitionVisualizer) drawMutableKeyRange(partitions []*Partition) {
	var timelinePoints []time.Time
	for t := range v.timestamps {
		timelinePoints = append(timelinePoints, t)
	}
	sort.Slice(timelinePoints, func(i, j int) bool {
		return timelinePoints[i].Before(timelinePoints[j])
	})

	// Assign aliases
	tokenToAlias := make(map[string]string)
	aliasCount := 1
	for _, partition := range partitions {
		if partition.Token == rootPartitionToken {
			continue
		}
		alias := fmt.Sprintf("P%d", aliasCount)
		tokenToAlias[partition.Token] = alias
		aliasCount++
	}

	// Draw timeline spine
	for i, t := range timelinePoints {
		fmt.Fprintf(v.out, `  t%d [label="%s", shape=plaintext];`, i, t.Format(time.RFC3339))
		fmt.Fprintln(v.out, "")
	}
	for i := 0; i < len(timelinePoints)-1; i++ {
		fmt.Fprintf(v.out, `  t%d -> t%d [style=invis];`, i, i+1)
		fmt.Fprintln(v.out, "")
	}

	// Draw partition bars
	rankGroups := make(map[int][]string)
	for _, partition := range partitions {
		if partition.Token == rootPartitionToken {
			continue
		}
		alias := tokenToAlias[partition.Token]
		startBar := partition.StartTimestamp
		endBar := partition.EndTimestamp
		if endBar.IsZero() {
			endBar = timelinePoints[len(timelinePoints)-1]
		}

		var activeNodes []string
		for idx, t := range timelinePoints {
			if (t.After(startBar) || t.Equal(startBar)) && (t.Before(endBar) || t.Equal(endBar)) {
				nodeID := fmt.Sprintf("%s_t%d", alias, idx)
				activeNodes = append(activeNodes, nodeID)
				rankGroups[idx] = append(rankGroups[idx], nodeID)

				// Style
				label := ""
				shape := "point"
				if t.Equal(startBar) {
					label = alias
					shape = "box"
				} else if t.Equal(partition.EndTimestamp) {
					label = fmt.Sprintf("%s (ended)", alias)
					shape = "box"
				}

				if shape == "point" {
					fmt.Fprintf(v.out, `  "%s" [shape=point];`, nodeID)
				} else {
					fmt.Fprintf(v.out, `  "%s" [label="%s", shape=%s];`, nodeID, label, shape)
				}
				fmt.Fprintln(v.out, "")
			}
		}

		// Link active nodes vertically
		for i := 0; i < len(activeNodes)-1; i++ {
			fmt.Fprintf(v.out, `  "%s" -> "%s" [penwidth=3, arrowhead=none];`, activeNodes[i], activeNodes[i+1])
			fmt.Fprintln(v.out, "")
		}
	}

	// Align with spine
	var rankIndices []int
	for idx := range rankGroups {
		rankIndices = append(rankIndices, idx)
	}
	sort.Ints(rankIndices)
	for _, idx := range rankIndices {
		nodes := rankGroups[idx]
		fmt.Fprintf(v.out, "  { rank=same; t%d; ", idx)
		for _, node := range nodes {
			fmt.Fprintf(v.out, `"%s"; `, node)
		}
		fmt.Fprintf(v.out, "}\n")
	}

	// Draw move edges
	for _, m := range v.moves {
		idx := -1
		for i, t := range timelinePoints {
			if t.Equal(m.Timestamp) {
				idx = i
				break
			}
		}
		if idx != -1 {
			sourceAlias := tokenToAlias[m.Source]
			destAlias := tokenToAlias[m.Dest]
			fmt.Fprintf(v.out, `  "%s_t%d" -> "%s_t%d" [style=dashed, constraint=false, label="move"];`, sourceAlias, idx, destAlias, idx)
			fmt.Fprintln(v.out, "")
		}
	}

	// Draw legend
	legend := "Partition Mapping:\\n"
	for _, partition := range partitions {
		if partition.Token == rootPartitionToken {
			continue
		}
		alias := tokenToAlias[partition.Token]
		legend += fmt.Sprintf("%s: %s\\n", alias, partition.Token)
	}
	fmt.Fprintf(v.out, "  label=\"%s\";\n", legend)
	fmt.Fprintf(v.out, "  labelloc=\"b\";\n")
}

func sortPartitions(partitionsMap map[string]*Partition) []*Partition {
	var partitions []*Partition
	for _, p := range partitionsMap {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Token < partitions[j].Token
	})
	return partitions
}
