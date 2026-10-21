// Тесты идентификатора держателя аренды (D13): locked_by адресует слот, а
// не процесс. При одном слоте вид locked_by остаётся прежним — суффикса
// нет; при нескольких слотах каждый слот получает свой идентификатор,
// иначе guard locked_by = $me внутри процесса не отличил бы попытку от
// попытки. База не нужна: проверяется чистая функция движка.
package brooq

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestWorkerID — workerID(slot): Workers <= 1 не добавляет суффикс,
// Workers > 1 даёт разные значения на слот с базой в начале.
func TestWorkerID(t *testing.T) {
	const base = "worker-base"

	t.Run("один воркер: базовый идентификатор без суффикса", func(t *testing.T) {
		q := New(nil, discardLogger(), Config{Workers: 1, WorkerID: base})
		for slot := 0; slot < 3; slot++ {
			assert.Equal(t, base, q.workerID(slot),
				"при одном слоте locked_by не зависит от номера слота")
		}
	})

	t.Run("нулевые настройки приводятся к одному воркеру", func(t *testing.T) {
		// New подставляет defaultWorkers=1: суффикса быть не должно.
		q := New(nil, discardLogger(), Config{WorkerID: base})
		assert.Equal(t, base, q.workerID(0))
	})

	t.Run("три воркера: три разных идентификатора с базой в начале", func(t *testing.T) {
		q := New(nil, discardLogger(), Config{Workers: 3, WorkerID: base})
		ids := make([]string, 0, 3)
		for slot := 0; slot < 3; slot++ {
			ids = append(ids, q.workerID(slot))
		}

		seen := make(map[string]struct{}, len(ids))
		for slot, id := range ids {
			assert.True(t, strings.HasPrefix(id, base),
				"идентификатор слота %d (%q) обязан начинаться с базы %q", slot, id, base)
			assert.NotEqual(t, base, id,
				"идентификатор слота %d обязан отличаться от базы", slot)
			seen[id] = struct{}{}
		}
		assert.Len(t, seen, 3, "три слота — три разных locked_by, получено %v", ids)
		assert.ElementsMatch(t, []string{base + "-0", base + "-1", base + "-2"}, ids,
			"формат идентификатора слота — <база>-<слот>")
	})

	t.Run("явный WorkerID — база для суффиксов", func(t *testing.T) {
		q := New(nil, discardLogger(), Config{Workers: 2, WorkerID: "explicit-worker"})
		assert.Equal(t, "explicit-worker-0", q.workerID(0))
		assert.Equal(t, "explicit-worker-1", q.workerID(1))
	})

	t.Run("пустой WorkerID генерируется и тоже различает слоты", func(t *testing.T) {
		q := New(nil, discardLogger(), Config{Workers: 2})
		baseID := q.workerID(0)
		assert.NotEmpty(t, baseID, "пустой WorkerID заменяется сгенерированным")
		assert.True(t, strings.HasSuffix(q.workerID(1), "-1"),
			"второй слот получает суффикс: %q", q.workerID(1))
		assert.NotEqual(t, baseID, q.workerID(1))
	})
}
