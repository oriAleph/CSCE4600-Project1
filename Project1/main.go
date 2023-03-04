package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		BurstDuration int64
		Burst         int64 //original burst duration
		ArrivalTime   int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// Scheduling functions outputs a schedule of processes
// in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes

func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		compareNext     int64
		compare         = true
		onHold          []Process
		count           = float64(len(processes))
	)
	for i := range processes {

		//Current process waiting time
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		//Current total waiting
		totalWait += float64(waitingTime)

		//Process start time
		var start = waitingTime + processes[i].ArrivalTime

		//Compare to other processes that have not arrived
		if compare {
			for c := i + 1; c < len(processes); c++ {
				compareNext = start + processes[i].BurstDuration - processes[c].ArrivalTime
				//If next process will arrive before current process completion
				if compareNext > 0 {
					//If next process has a shorter job
					if processes[i].BurstDuration > processes[c].BurstDuration {

						//Add current process to onHold list
						processes[i].BurstDuration -= (processes[c].ArrivalTime - start)
						onHold = append(onHold, processes[i])
						sort.Sort(ByBurst(onHold))

						//Gantt Schedule
						serviceTime += (processes[c].ArrivalTime - start)
						gantt = append(gantt, TimeSlice{
							PID:   processes[i].ProcessID,
							Start: start,
							Stop:  serviceTime,
						})

						//Delete current process from processes list
						processes = append(processes[:i], processes[i+1:]...)
						c--

						//New current process waiting time
						if processes[i].ArrivalTime > 0 {
							waitingTime = serviceTime - processes[i].ArrivalTime
						}

						//New current total waiting
						totalWait += float64(waitingTime)

						//New process start time
						start = waitingTime + processes[i].ArrivalTime

					} else { //If next process does not have a shorter job

						//Add next process to onHold list
						onHold = append(onHold, processes[c])
						sort.Sort(ByBurst(onHold))

						//Delete next process from processes list
						processes = append(processes[:c], processes[c+1:]...)
						c--
					}
				} else {
					break
				}
			}
		}

		//Gantt Schedule
		serviceTime += processes[i].BurstDuration
		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})

		//Schedule Table
		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].Burst),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}

		//Prepend preemptive/waiting process
		if compare && len(onHold) > 0 && i != (len(processes)-1) {
			if onHold[0].ProcessID != processes[i+1].ProcessID {
				processes = Insert(processes, i+1, onHold[0])
			}
			if len(onHold) > 1 {
				onHold = append(onHold[:0], onHold[1:]...)
			} else {
				onHold = nil
			}
		}

		// Processes left to run
		if i == (len(processes) - 1) {
			processes = append(processes, onHold...)
			onHold = nil
			compare = false
		}
	}

	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		compareNext     int64
		compare         = true
		onHold          []Process
		count           = float64(len(processes))
	)
	sort.Sort(ByArrival(processes))
	for i := range processes {

		//Current process waiting time
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		//Current total waiting
		totalWait += float64(waitingTime)

		//Process start time
		var start = waitingTime + processes[i].ArrivalTime

		//Compare to other processes that have not arrived
		if compare {
			for c := i + 1; c < len(processes); c++ {
				compareNext = start + processes[i].BurstDuration - processes[c].ArrivalTime
				//If next process will arrive before current process completion
				if compareNext > 0 {
					//If next process has higher priority
					if processes[i].Priority < processes[c].Priority {

						//Add current process to onHold list
						processes[i].BurstDuration -= (processes[c].ArrivalTime - start)
						onHold = append(onHold, processes[i])
						sort.Sort(ByPriority(onHold))

						//Gantt Schedule
						serviceTime += (processes[c].ArrivalTime - start)
						gantt = append(gantt, TimeSlice{
							PID:   processes[i].ProcessID,
							Start: start,
							Stop:  serviceTime,
						})

						//Delete current process from processes list
						processes = append(processes[:i], processes[i+1:]...)
						c--

						//New current process waiting time
						if processes[i].ArrivalTime > 0 {
							waitingTime = serviceTime - processes[i].ArrivalTime
						}

						//New current total waiting
						totalWait += float64(waitingTime)

						//New process start time
						start = waitingTime + processes[i].ArrivalTime

					} else { //If next process does not higher priority

						//Add next process to onHold list
						onHold = append(onHold, processes[c])
						sort.Sort(ByPriority(onHold))

						//Delete next process from processes list
						processes = append(processes[:c], processes[c+1:]...)
						c--
					}
				} else {
					break
				}
			}
		}

		//Gantt Schedule
		serviceTime += processes[i].BurstDuration
		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})

		//Schedule Table
		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].Burst),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}

		//Prepend preemptive/waiting process
		if compare && len(onHold) > 0 && i != (len(processes)-1) {
			if onHold[0].ProcessID != processes[i+1].ProcessID {
				processes = Insert(processes, i+1, onHold[0])
			}
			if len(onHold) > 1 {
				onHold = append(onHold[:0], onHold[1:]...)
			} else {
				onHold = nil
			}
		}

		// Processes left to run
		if i == (len(processes) - 1) {
			processes = append(processes, onHold...)
			onHold = nil
			compare = false
		}
	}

	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		count           = float64(len(processes))
		quantum         = int64(3)
		scheduleCount   = 0
	)
	sort.Sort(ByArrival(processes))
	for i := 0; i < len(processes); i++ {

		//Current process waiting time
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		//Current total waiting
		totalWait += float64(waitingTime)

		//Process start time
		var start = waitingTime + processes[i].ArrivalTime

		//Gantt Schedule
		serviceTime += quantum
		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})

		if processes[i].BurstDuration-quantum <= 0 {
			processes[i].BurstDuration = 0
		} else {
			processes[i].BurstDuration -= quantum
			processes = append(processes, processes[i])
		}

		if processes[i].BurstDuration == 0 {
			//Schedule Table
			turnaround := processes[i].Burst + waitingTime
			totalTurnaround += float64(turnaround)

			completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
			lastCompletion = float64(completion)

			schedule[scheduleCount] = []string{
				fmt.Sprint(processes[i].ProcessID),
				fmt.Sprint(processes[i].Priority),
				fmt.Sprint(processes[i].Burst),
				fmt.Sprint(processes[i].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}
			scheduleCount++
		}
	}

	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].Burst = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	sort.Sort(ByArrival(processes))
	return processes, nil
}

// ByArrival implements sort.Interface based on the ArrivalTime field
type ByArrival []Process

func (a ByArrival) Len() int           { return len(a) }
func (a ByArrival) Less(i, j int) bool { return a[i].ArrivalTime < a[j].ArrivalTime }
func (a ByArrival) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// ByBurst implements sort.Interface based on the BurstDuration field
type ByBurst []Process

func (a ByBurst) Len() int           { return len(a) }
func (a ByBurst) Less(i, j int) bool { return a[i].BurstDuration < a[j].BurstDuration }
func (a ByBurst) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// ByPriority implements sort.Interface based on the Priority field
type ByPriority []Process

func (a ByPriority) Len() int           { return len(a) }
func (a ByPriority) Less(i, j int) bool { return a[i].Priority < a[j].Priority }
func (a ByPriority) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// Insert Process
func Insert(a []Process, index int, value Process) []Process {
	if len(a) == index { // nil or empty slice or after last element
		return append(a, value)
	}
	a = append(a[:index+1], a[index:]...) // index < len(a)
	a[index] = value
	return a
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
