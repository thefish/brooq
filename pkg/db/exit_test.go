package db_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/thefish/brooq/pkg/db"
)

// exitDropSchema is the schema name for testing; it is fixed, so the test
// is idempotent (the schema is dropped at the start and verified at the end).
const exitDropSchema = "brooq_exitdrop_test"

// TestExitWithSchemaDrop fixes testing schema leaks: the helper
// db.ExitWithSchemaDrop must drop the schema and exit the process with code 1.
// The helper calls os.Exit, so the check runs in a child process of the same test binary.
func TestExitWithSchemaDrop(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	bootstrap, err := db.Connect(ctx, dsn, db.DefaultParams())
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	defer bootstrap.Close()
	if err := db.DropSchemaWithGuard(ctx, bootstrap, exitDropSchema); err != nil {
		t.Fatalf("failed to prepare schema: %v", err)
	}
	if _, err := bootstrap.Exec(ctx, "CREATE SCHEMA "+exitDropSchema); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestExitWithSchemaDropChild") //nolint:gosec // G204: sub-test binary, not user input
	cmd.Env = append(os.Environ(), "RASPISATR_EXIT_DROP_CHILD=1")
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1 from helper: err=%v, output=%s", err, out)
	}

	var exists bool
	if err := bootstrap.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", exitDropSchema).Scan(&exists); err != nil {
		t.Fatalf("failed to verify schema: %v", err)
	}
	if exists {
		t.Errorf("schema %s was not dropped by ExitWithSchemaDrop", exitDropSchema)
	}
}

// TestExitWithSchemaDropChild is the body of the child process: it calls the helper
// on the schema created by the parent. If the code after the call is reached,
// the helper failed to exit the process.
func TestExitWithSchemaDropChild(t *testing.T) {
	if os.Getenv("RASPISATR_EXIT_DROP_CHILD") != "1" {
		t.Skip("not a child process")
	}
	ctx := context.Background()
	bootstrap, err := db.Connect(ctx, os.Getenv("TEST_DATABASE_URL"), db.DefaultParams())
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	db.ExitWithSchemaDrop(ctx, bootstrap, exitDropSchema)
	t.Fatal("ExitWithSchemaDrop failed to exit the process")
}
