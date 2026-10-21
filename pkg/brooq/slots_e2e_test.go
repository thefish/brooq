// Тест слотов на настоящей PostgreSQL: locked_by адресует держателя
// аренды, а не процесс (D13). Движок с двумя слотами на одной теме берёт
// два задания — и в locked_by этих заданий обязаны оказаться два разных
// значения. До правки workerID оба задания получили бы идентификатор
// процесса, и guard locked_by = $me перестал бы отличать попытку от
// попытки внутри процесса.
//
// Без TEST_DATABASE_URL тест пропускается (newTestPool → t.Skip).
package brooq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

// leaseRecorder — обработчик, запоминающий locked_by взятого задания под
// мьютексом (слоты работают в разных горутинах). Попытку он держит до
// release: иначе один слот успел бы обработать оба задания подряд, и
// тест доказывал бы не различие слотов, а скорость планировщика.
type leaseRecorder struct {
	mu      sync.Mutex
	workers []string

	// started сигнализирует о начале попытки; буфер 2, поэтому третий
	// вызов не заблокировал бы обработчик.
	started chan struct{}
	// release освобождает все удерживаемые попытки.
	release chan struct{}
}

// newLeaseRecorder собирает записывающий обработчик.
func newLeaseRecorder() *leaseRecorder {
	return &leaseRecorder{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
}

// Handle запоминает locked_by и ждёт release (или отмены попытки).
func (h *leaseRecorder) Handle(ctx context.Context, c queue.Claim) error {
	h.mu.Lock()
	h.workers = append(h.workers, c.Lease.Worker)
	h.mu.Unlock()

	select {
	case h.started <- struct{}{}:
	default:
	}

	select {
	case <-h.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// snapshot возвращает запомненные locked_by в порядке вызовов.
func (h *leaseRecorder) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.workers...)
}

// TestSlotsLockedByDistinct — два слота одного движка берут два задания,
// и locked_by у них различаются: идентификатор держателя аренды
// уникален на слот.
func TestSlotsLockedByDistinct(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "e2e-slots")

	// base — явный WorkerID: проверяется вид <база>-<слот>, а не хост и pid.
	const base = "e2e-slots-base"
	want := []string{base + "-0", base + "-1"}

	recorder := newLeaseRecorder()
	engine := newEngineWith(t, repo, topic, recorder, Config{
		Workers:      2,
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		WorkerID:     base,
	})
	defer func() { _ = engine.Close() }()

	require.NoError(t, repo.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"n":1}`),
	}), "постановка первого задания")
	require.NoError(t, repo.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"n":2}`),
	}), "постановка второго задания")

	engine.Start()

	// Оба слота обязаны взяться за дело: два задания, два свободных места.
	for i := 0; i < 2; i++ {
		select {
		case <-recorder.started:
		case <-time.After(e2eTimeout):
			t.Fatalf("обработчик вызван %d раз(а) за %s, ожидалось 2: два слота обязаны взять по заданию",
				len(recorder.snapshot()), e2eTimeout)
		}
	}
	close(recorder.release)

	waitDoneJobs(t, pool, topic, 2)

	workers := recorder.snapshot()
	require.Len(t, workers, 2, "обработчик вызван дважды — по разу на задание")
	assert.NotEqual(t, workers[0], workers[1],
		"locked_by двух заданий обязан различаться: это идентификатор слота, а не процесса")
	assert.ElementsMatch(t, want, workers,
		"locked_by — <база>-<слот>, база берётся из Config.WorkerID")

	// То же самое в самой строке задания: доказывается запись, а не
	// только значение, увиденное обработчиком.
	rows, err := pool.Query(ctx,
		`SELECT DISTINCT COALESCE(locked_by, '') FROM job WHERE topic = $1`, string(topic))
	require.NoError(t, err, "чтение locked_by заданий темы %s", topic)
	defer rows.Close()

	var lockedBy []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		lockedBy = append(lockedBy, id)
	}
	require.NoError(t, rows.Err())

	assert.ElementsMatch(t, want, lockedBy,
		"в job.locked_by два разных слота одного процесса")
}

// waitDoneJobs ждёт, пока в теме наберётся want заданий в состоянии done.
func waitDoneJobs(t *testing.T, pool *pgxpool.Pool, topic queue.Topic, want int) {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(e2eTimeout)
	var done int
	for {
		err := pool.QueryRow(ctx,
			`SELECT count(*) FROM job WHERE topic = $1 AND state = $2`,
			string(topic), string(queue.StateDone),
		).Scan(&done)
		require.NoError(t, err, "чтение числа завершённых заданий темы %s", topic)
		if done >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("заданий темы %s в состоянии done: %d, ожидалось %d за %s",
				topic, done, want, e2eTimeout)
		}
		time.Sleep(e2ePollInterval)
	}
}
