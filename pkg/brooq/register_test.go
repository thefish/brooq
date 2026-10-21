// Тесты регистрации темы (D9, retry/spec.md «Потолок попыток проверяется
// при регистрации темы»): некорректный потолок попыток и повторная
// регистрация той же темы отклоняются на этапе регистрации, а не при
// исполнении. База не нужна — проверяется решение движка поверх фейкового
// хранилища.
package brooq

import (
	"context"
	"strings"
	"testing"

	"github.com/thefish/brooq/pkg/queue"
)

// TestRegisterKindValidation — таблица «последовательность регистраций →
// результат последней». Все шаги, кроме последнего, обязаны проходить:
// иначе проверялась бы не та ошибка.
func TestRegisterKindValidation(t *testing.T) {
	// step — одна попытка регистрации: тема и её потолок попыток.
	type step struct {
		topic       queue.Topic
		maxAttempts int
	}

	// ok — обработчик-заглушка: регистрация его не исполняет.
	ok := handlerFunc(func(context.Context, queue.Claim) error { return nil })

	// registered сообщает, попала ли тема в набор движка: без этого
	// «ошибки нет» ещё не доказывает, что регистрация состоялась.
	registered := func(q *Queue, topic queue.Topic) bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		_, found := q.kinds[topic]
		return found
	}

	tests := []struct {
		name string
		// steps — последовательные регистрации.
		steps []step
		// wantErr — подстрока ошибки последней регистрации ("" — ошибки
		// быть не должно).
		wantErr string
	}{
		{
			name:    "потолок попыток 0 отклоняется",
			steps:   []step{{"тема", 0}},
			wantErr: "потолок попыток",
		},
		{
			name:    "потолок попыток -1 отклоняется",
			steps:   []step{{"тема", -1}},
			wantErr: "потолок попыток",
		},
		{
			name:  "потолок 1 принимается",
			steps: []step{{"тема", 1}},
		},
		{
			name:  "потолок 3 принимается",
			steps: []step{{"тема", 3}},
		},
		{
			name:    "повторная регистрация той же темы отклоняется",
			steps:   []step{{"тема", 1}, {"тема", 1}},
			wantErr: "уже зарегистрирована",
		},
		{
			name:    "повторная регистрация с другими настройками тоже отклоняется",
			steps:   []step{{"тема", 1}, {"тема", 5}},
			wantErr: "уже зарегистрирована",
		},
		{
			name:  "разные темы регистрируются",
			steps: []step{{"тема-a", 1}, {"тема-b", 2}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q := New(newFakeRepo(), discardLogger(), Config{Workers: 1, WorkerID: "test-worker"})

			var err error
			for i, s := range tc.steps {
				err = q.RegisterKind(s.topic, ok, queue.KindOptions{MaxAttempts: s.maxAttempts})
				if i == len(tc.steps)-1 {
					continue
				}
				if err != nil {
					t.Fatalf("шаг %d (тема %q, потолок %d) обязан пройти: %v",
						i, s.topic, s.maxAttempts, err)
				}
			}

			last := tc.steps[len(tc.steps)-1]
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("регистрация темы %q с потолком %d обязана пройти, получена ошибка: %v",
						last.topic, last.maxAttempts, err)
				}
				if !registered(q, last.topic) {
					t.Fatalf("тема %q не попала в набор движка после успешной регистрации", last.topic)
				}
				return
			}

			if err == nil {
				t.Fatalf("регистрация темы %q с потолком %d обязана завершиться ошибкой",
					last.topic, last.maxAttempts)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ошибка = %q, ожидалась подстрока %q", err, tc.wantErr)
			}
		})
	}

	t.Run("отклонённая регистрация не оставляет тему в наборе", func(t *testing.T) {
		q := New(newFakeRepo(), discardLogger(), Config{Workers: 1, WorkerID: "test-worker"})
		if err := q.RegisterKind("тема", ok, queue.KindOptions{MaxAttempts: 0}); err == nil {
			t.Fatalf("регистрация с потолком 0 обязана завершиться ошибкой")
		}
		if registered(q, "тема") {
			t.Fatalf("отклонённая тема осталась в наборе движка")
		}
	})

	t.Run("повторная регистрация не подменяет настройки первой", func(t *testing.T) {
		q := New(newFakeRepo(), discardLogger(), Config{Workers: 1, WorkerID: "test-worker"})
		if err := q.RegisterKind("тема", ok, queue.KindOptions{MaxAttempts: 2}); err != nil {
			t.Fatalf("первая регистрация обязана пройти: %v", err)
		}
		if err := q.RegisterKind("тема", ok, queue.KindOptions{MaxAttempts: 7}); err == nil {
			t.Fatalf("повторная регистрация обязана завершиться ошибкой")
		}

		q.mu.Lock()
		got := q.kinds["тема"].opts.MaxAttempts
		q.mu.Unlock()
		if got != 2 {
			t.Fatalf("потолок попыток после отклонённой повторной регистрации = %d, ожидался 2", got)
		}
	})
}
