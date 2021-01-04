package main

import (
	"os/exec"
	"testing"
)

func TestMain_CanStartWhenPostgresIsDown(t *testing.T) {
	// Make sure pgreba is built
	cmd := exec.Command("go", "build")
	buildErr := cmd.Run()
	if buildErr != nil {
		t.Fatal("Failed to build")
	}

	// Stop running postgresql
	stopPostgres := exec.Command("brew", "services", "stop", "postgresql")
	stopPostgresErr := stopPostgres.Run()
	if stopPostgresErr != nil {
		t.Fatal("Failed to stop postgres")
	}

	// Run pgreba
	runPgreba := exec.Command("./pgreba", "examples/local.yml")
	startErr := runPgreba.Start()
	defer runPgreba.Process.Kill()
	if startErr != nil {
		t.Fatal("Pgreba failed to start")
	}

	// Start running postgresql
	startPostgres := exec.Command("brew", "services", "start", "postgresql")
	startPostgresErr := startPostgres.Run()
	if startPostgresErr != nil {
		t.Fatal("failed to start postgres")
	}

	// Make request
	getPrimary := exec.Command("curl", "-s", "http://localhost:8000/ | jq .")
	getPrimaryErr := getPrimary.Run()
	if getPrimaryErr != nil {
		t.Fatal("Should get a status 200 back")
	}
}
