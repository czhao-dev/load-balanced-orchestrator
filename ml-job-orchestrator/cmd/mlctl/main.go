// mlctl is a stdlib-only CLI client for the ml-job-orchestrator REST API.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/czhao-dev/ml-job-orchestrator/internal/model"
)

func serverURL() string {
	if v := os.Getenv("MLCTL_SERVER"); v != "" {
		return v
	}
	return "http://localhost:8080"
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "submit":
		cmdSubmit(os.Args[2:])
	case "status":
		cmdStatus(os.Args[2:])
	case "list":
		cmdList(os.Args[2:])
	case "cancel":
		cmdCancel(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `mlctl <command> [flags]

Commands:
  submit   submit a new job
  status   print a job's current status
  list     list jobs, optionally filtered
  cancel   cancel a job

Set MLCTL_SERVER (default http://localhost:8080) to point at a different orchestrator.`)
}

func cmdSubmit(args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	name := fs.String("name", "", "job name (required)")
	jobType := fs.String("type", "", "job type, e.g. training")
	command := fs.String("command", "", "command to execute (required)")
	// A single space-delimited string, not repeatable flags, to match the
	// README's `--args "train.py --epochs 5"` usage. This means an argument
	// containing a literal space cannot be represented.
	argsStr := fs.String("args", "", "space-separated command arguments")
	timeout := fs.Int("timeout", 0, "timeout in seconds (0 = no timeout)")
	retries := fs.Int("retries", 0, "max retry attempts")
	priority := fs.Int("priority", 0, "scheduling priority (higher runs first)")
	fs.Parse(args)

	if *name == "" || *command == "" {
		fmt.Fprintln(os.Stderr, "submit: --name and --command are required")
		os.Exit(1)
	}

	var jobArgs []string
	if strings.TrimSpace(*argsStr) != "" {
		jobArgs = strings.Fields(*argsStr)
	}

	body, _ := json.Marshal(map[string]any{
		"name":            *name,
		"type":            *jobType,
		"command":         *command,
		"args":            jobArgs,
		"timeout_seconds": *timeout,
		"max_retries":     *retries,
		"priority":        *priority,
	})

	resp, err := http.Post(serverURL()+"/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		fatalf("submit failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		fatalf("submit failed: %s", readErrorBody(resp))
	}

	var created struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		fatalf("submit: failed to decode response: %v", err)
	}
	fmt.Printf("Submitted %s\n", created.ID)
}

func cmdStatus(args []string) {
	if len(args) < 1 {
		fatalf("status: job id required")
	}
	job := fetchJob(args[0])

	fmt.Printf("ID:       %s\n", job.ID)
	fmt.Printf("Name:     %s\n", job.Name)
	fmt.Printf("State:    %s\n", job.State)
	fmt.Printf("Duration: %s\n", formatDuration(job))
	fmt.Printf("Retries:  %d\n", job.RetryCount)
	if job.ErrorMessage != "" {
		fmt.Printf("Error:    %s\n", job.ErrorMessage)
	}
}

func formatDuration(job model.Job) string {
	switch {
	case job.StartedAt == nil:
		return "-"
	case job.FinishedAt != nil:
		return job.FinishedAt.Sub(*job.StartedAt).Round(time.Millisecond).String()
	default:
		return time.Since(*job.StartedAt).Round(time.Millisecond).String() + " (running)"
	}
}

func cmdList(args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	state := fs.String("state", "", "filter by state")
	jobType := fs.String("type", "", "filter by type")
	limit := fs.Int("limit", 20, "max results")
	fs.Parse(args)

	url := fmt.Sprintf("%s/jobs?state=%s&type=%s&limit=%d", serverURL(), *state, *jobType, *limit)
	resp, err := http.Get(url)
	if err != nil {
		fatalf("list failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fatalf("list failed: %s", readErrorBody(resp))
	}

	var result struct {
		Jobs  []model.Job `json:"jobs"`
		Total int         `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fatalf("list: failed to decode response: %v", err)
	}

	fmt.Printf("%-12s %-20s %-10s %-10s\n", "ID", "NAME", "STATE", "RETRIES")
	for _, j := range result.Jobs {
		fmt.Printf("%-12s %-20s %-10s %-10d\n", j.ID, j.Name, j.State, j.RetryCount)
	}
	fmt.Printf("\n%d total\n", result.Total)
}

func cmdCancel(args []string) {
	if len(args) < 1 {
		fatalf("cancel: job id required")
	}
	id := args[0]
	req, _ := http.NewRequest(http.MethodDelete, serverURL()+"/jobs/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatalf("cancel failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fatalf("cancel failed: %s", readErrorBody(resp))
	}
	fmt.Printf("Cancelled %s\n", id)
}

func fetchJob(id string) model.Job {
	resp, err := http.Get(serverURL() + "/jobs/" + id)
	if err != nil {
		fatalf("status failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fatalf("status failed: %s", readErrorBody(resp))
	}
	var job model.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		fatalf("status: failed to decode response: %v", err)
	}
	return job
}

func readErrorBody(resp *http.Response) string {
	b, _ := io.ReadAll(resp.Body)
	return fmt.Sprintf("%s: %s", resp.Status, strings.TrimSpace(string(b)))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
