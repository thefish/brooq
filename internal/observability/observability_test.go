// Тесты наблюдаемости: накопление счётчиков с лейблами,
// рендер Prometheus text (HELP/TYPE/значения, сортировка, экранирование),
// middleware латентности/ошибок (код ответа попадает в метрики).
package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bodyMetrics рендерит /metrics и возращает текст.
func bodyMetrics(t *testing.T, r *Registry) string {
	t.Helper()
	w := httptest.NewRecorder()
	r.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain")
	return w.Body.String()
}

// countValueLines считает строки значений (не # HELP/# TYPE) для имени.
func countValueLines(body, name string) int {
	n := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") || strings.HasPrefix(line, name+"{") {
			n++
		}
	}
	return n
}

func TestRegistryCounters(t *testing.T) {
	r := New()
	r.Inc("brooq_jobs_total", "Задания очереди", map[string]string{"topic": "email", "state": "claimed"})
	r.Inc("brooq_jobs_total", "", map[string]string{"topic": "email", "state": "done"})
	r.Inc("brooq_jobs_total", "", map[string]string{"topic": "webhook", "state": "dead"})

	body := bodyMetrics(t, r)
	assert.Contains(t, body, `# TYPE brooq_jobs_total counter`)
	assert.Contains(t, body, `brooq_jobs_total{state="claimed",topic="email"} 1`)
	assert.Contains(t, body, `brooq_jobs_total{state="done",topic="email"} 1`)
	assert.Contains(t, body, `brooq_jobs_total{state="dead",topic="webhook"} 1`)
	// Лейблы сортируются (детерминизм вывода).
	claimed := strings.Index(body, `state="claimed",topic="email"`)
	assert.Greater(t, claimed, -1)
}

func TestRegistryEscaping(t *testing.T) {
	r := New()
	r.Inc("brooq_claims_total", "Взятия заданий", map[string]string{"worker": `a"b\c`})
	body := bodyMetrics(t, r)
	assert.Contains(t, body, `brooq_claims_total{worker="a\"b\\c"} 1`)
}

func TestRegistryStableOrder(t *testing.T) {
	r := New()
	r.Inc("b", "", nil)
	r.Inc("a", "", nil)
	r.Inc("c", "", nil)
	body := bodyMetrics(t, r)
	// Имена сортируются (детерминизм вывода).
	assert.True(t, strings.Contains(body, "a "))
	assert.Less(t, strings.Index(body, "a "), strings.Index(body, "b "))
	assert.Less(t, strings.Index(body, "b "), strings.Index(body, "c "))
}

func TestMiddlewareLatencyAndStatus(t *testing.T) {
	r := New()
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "x")
	})
	srv := httptest.NewServer(r.Middleware(inner))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/x")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusTeapot, resp.StatusCode)

	body := bodyMetrics(t, r)
	assert.Contains(t, body, `brooq_http_requests_total{method="GET",status="418"} 1`)
	assert.Contains(t, body, `brooq_http_request_duration_ms_sum{method="GET"} `)
	assert.Contains(t, body, `brooq_http_request_duration_ms_count{method="GET"} 1`)
}

// TestRegistryGaugeSetAbsolute — gauge хранит «сколько сейчас»: Set задаёт
// абсолютное значение, поэтому повторный Set перезаписывает, а не
// суммирует. Таблица фиксирует границы (повтор, ноль, лейблы) и конфликт
// типа: Add/Inc по имени, занятому gauge, игнорируется (fixKind) — значение
// gauge не меняется, вторая серия не создаётся.
func TestRegistryGaugeSetAbsolute(t *testing.T) {
	tests := []struct {
		name            string
		setup           func(r *Registry)
		wantContains    []string
		wantNotContains []string
		wantValueLines  map[string]int
	}{
		{
			name: "повторный Set перезаписывает, а не накапливает",
			setup: func(r *Registry) {
				r.Set("brooq_ready_jobs", "Готовые задания", nil, 5)
				r.Set("brooq_ready_jobs", "", nil, 7)
			},
			wantContains:    []string{"brooq_ready_jobs 7"},
			wantNotContains: []string{"brooq_ready_jobs 12"},
		},
		{
			name: "Set нулём — граница (значение, а не накопление)",
			setup: func(r *Registry) {
				r.Set("brooq_ready_oldest_age_seconds", "Возраст старейшего", nil, 30)
				r.Set("brooq_ready_oldest_age_seconds", "", nil, 0)
			},
			wantContains:    []string{"brooq_ready_oldest_age_seconds 0"},
			wantNotContains: []string{"brooq_ready_oldest_age_seconds 30"},
		},
		{
			name: "Set с лейблами перезаписывает ту же серию",
			setup: func(r *Registry) {
				r.Set("brooq_ready_jobs", "Готовые задания", map[string]string{"topic": "email"}, 3)
				r.Set("brooq_ready_jobs", "", map[string]string{"topic": "email"}, 9)
			},
			wantContains:    []string{`brooq_ready_jobs{topic="email"} 9`},
			wantNotContains: []string{`brooq_ready_jobs{topic="email"} 12`},
		},
		{
			// Конфликт типа: Inc/Add по имени, занятому gauge, игнорируется
			// (fixKind) — значение gauge не меняется, вторая серия не заводится.
			name: "Inc после Set игнорируется — gauge остаётся 5",
			setup: func(r *Registry) {
				r.Set("brooq_ready_jobs", "Готовые задания", nil, 5)
				r.Inc("brooq_ready_jobs", "", nil)
			},
			wantContains:    []string{"brooq_ready_jobs 5"},
			wantNotContains: []string{"brooq_ready_jobs 1", "brooq_ready_jobs 6"},
			wantValueLines:  map[string]int{"brooq_ready_jobs": 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := New()
			tc.setup(r)
			body := bodyMetrics(t, r)
			for _, want := range tc.wantContains {
				assert.Contains(t, body, want)
			}
			for _, notWant := range tc.wantNotContains {
				assert.NotContains(t, body, notWant)
			}
			for name, want := range tc.wantValueLines {
				assert.Equal(t, want, countValueLines(body, name),
					"строк значения для имени %s", name)
			}
		})
	}
}

// TestRegistryGaugeTypeAndLineFormat — # TYPE различает gauge и counter, а
// каждая строка значения имеет вид `name value` либо `name{...} value`.
// Значение дробное, поэтому строка формата допускает и дробную часть.
func TestRegistryGaugeTypeAndLineFormat(t *testing.T) {
	r := New()
	r.Set("brooq_ready_jobs", "Готовые задания", nil, 4)
	r.Set("brooq_ready_jobs", "", map[string]string{"topic": "email"}, 2)
	r.Set("brooq_ready_oldest_age_seconds", "Возраст старейшего", nil, 0.5)
	r.Inc("brooq_jobs_total", "Задания", map[string]string{"state": "done"})
	body := bodyMetrics(t, r)

	assert.Contains(t, body, "# TYPE brooq_ready_jobs gauge")
	assert.Contains(t, body, "# TYPE brooq_ready_oldest_age_seconds gauge")
	assert.Contains(t, body, "# TYPE brooq_jobs_total counter")

	lineRE := regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[a-zA-Z_][a-zA-Z0-9_]*="[^"]*"(,[a-zA-Z_][a-zA-Z0-9_]*="[^"]*")*\})? [0-9]+(\.[0-9]+)?$`)
	sawPlain, sawLabeled, sawFraction := false, false, false
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		require.Regexp(t, lineRE, line, "формат строки метрики: name value или name{...} value")
		if strings.Contains(line, "{") {
			sawLabeled = true
		} else {
			sawPlain = true
		}
		if strings.Contains(line, ".") {
			sawFraction = true
		}
	}
	assert.True(t, sawPlain, "нужна строка без лейблов (name value)")
	assert.True(t, sawLabeled, "нужна строка с лейблами (name{...} value)")
	assert.True(t, sawFraction, "нужна строка с дробным значением (name 0.5)")
}

// TestRegistryMetricsContentTypeVersion — /metrics отдаёт Prometheus
// text/plain; version=0.0.4.
func TestRegistryMetricsContentTypeVersion(t *testing.T) {
	r := New()
	r.Set("brooq_ready_jobs", "Готовые задания", nil, 1)
	w := httptest.NewRecorder()
	r.Handler(w, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain; version=0.0.4")
}

// TestRegistryFloatRendering — значение серии float64, и рендер обязан
// сохранить его смысл: дробное печатается дробно (0.25), целое — как
// целое (2), а не 2.000000. Хвост нулей не сломал бы парсер, но соврал бы
// о точности: 2.000000 читается как измерение с микросекундной
// точностью, которой не было.
func TestRegistryFloatRendering(t *testing.T) {
	const name = "brooq_ready_oldest_age_seconds"

	tests := []struct {
		name            string
		value           float64
		wantLine        string
		wantNotContains []string
	}{
		{
			name:            "дробное значение печатается дробно",
			value:           0.25,
			wantLine:        name + " 0.25",
			wantNotContains: []string{name + " 0.250000"},
		},
		{
			name:            "целое значение печатается как целое",
			value:           2,
			wantLine:        name + " 2",
			wantNotContains: []string{name + " 2.000000", name + " 2.0"},
		},
		{
			name:            "ноль — граница: 0, а не 0.000000",
			value:           0,
			wantLine:        name + " 0",
			wantNotContains: []string{name + " 0.000000"},
		},
		{
			// Тысячные доли — типичный возраст свежего задания: значение
			// не должно ни схлопнуться в 0, ни уйти в экспоненту.
			name:            "тысячные доли не схлопываются и не печатаются экспонентой",
			value:           0.001,
			wantLine:        name + " 0.001",
			wantNotContains: []string{name + " 0\n", name + " 1e-03"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := New()
			r.Set(name, "Возраст старейшего готового задания, с", nil, tc.value)
			body := bodyMetrics(t, r)
			assert.Contains(t, body, tc.wantLine)
			for _, notWant := range tc.wantNotContains {
				assert.NotContains(t, body, notWant)
			}
			assert.Equal(t, 1, countValueLines(body, name), "строка значения для имени ровно одна")
		})
	}
}

// TestRegistryAddFloatKeepsFraction — смысл перевода значений на float64:
// дробное приращение не теряется. AddFloat дважды по 0.25 даёт 0.5 (а не
// 0), Add поверх дробной суммы не сбрасывает накопленную дробь, а
// AddFloat по имени, занятому gauge, игнорируется — тип имени не
// подменяется.
func TestRegistryAddFloatKeepsFraction(t *testing.T) {
	const sum = "brooq_attempt_duration_seconds_sum"

	t.Run("два дробных приращения складываются в 0.5", func(t *testing.T) {
		r := New()
		r.AddFloat(sum, "Сумма длительностей попыток, с", nil, 0.25)
		r.AddFloat(sum, "", nil, 0.25)

		body := bodyMetrics(t, r)
		assert.Contains(t, body, "# TYPE "+sum+" counter")
		assert.Contains(t, body, sum+" 0.5")
		assert.NotContains(t, body, sum+" 0\n", "дробная часть потеряна: сумма стала нулём")
	})

	t.Run("целое приращение поверх дробной суммы не сбрасывает дробь", func(t *testing.T) {
		r := New()
		r.AddFloat(sum, "Сумма длительностей попыток, с", map[string]string{"topic": "email"}, 0.25)
		r.AddFloat(sum, "", map[string]string{"topic": "email"}, 0.25)
		r.Add(sum, "", map[string]string{"topic": "email"}, 1)

		body := bodyMetrics(t, r)
		assert.Contains(t, body, sum+`{topic="email"} 1.5`)
	})

	t.Run("AddFloat по имени gauge игнорируется — тип не подменяется", func(t *testing.T) {
		r := New()
		r.Set("brooq_worker_busy", "Занятые слоты", nil, 3)
		r.AddFloat("brooq_worker_busy", "", nil, 0.25)

		body := bodyMetrics(t, r)
		assert.Contains(t, body, "# TYPE brooq_worker_busy gauge")
		assert.Contains(t, body, "brooq_worker_busy 3")
		assert.NotContains(t, body, "# TYPE brooq_worker_busy counter")
		assert.NotContains(t, body, "brooq_worker_busy 3.25")
		assert.Equal(t, 1, countValueLines(body, "brooq_worker_busy"),
			"строка значения для имени ровно одна")
	})
}

// TestRegistrySetOnCounterNameDoesNotConvertCounter — Set по имени, уже
// занятому счётчиком, игнорируется (конфликт типа, fixKind): счётчик не
// подменяется и не меняется, вторая серия с тем же именем не создаётся,
// # TYPE с этим именем ровно один (counter).
func TestRegistrySetOnCounterNameDoesNotConvertCounter(t *testing.T) {
	r := New()
	r.Inc("brooq_jobs_total", "Задания", nil)
	r.Inc("brooq_jobs_total", "", nil) // счётчик = 2
	r.Set("brooq_jobs_total", "Глубина", nil, 8)

	body := bodyMetrics(t, r)

	// Счётчик не подменён: значение сохранилось, тип остался counter.
	assert.Contains(t, body, "# TYPE brooq_jobs_total counter")
	assert.Contains(t, body, "brooq_jobs_total 2")
	// Set проигнорирован: второй серии и второго # TYPE с этим именем нет.
	assert.NotContains(t, body, "# TYPE brooq_jobs_total gauge")
	assert.NotContains(t, body, "brooq_jobs_total 8")
	assert.Equal(t, 1, strings.Count(body, "# TYPE brooq_jobs_total "),
		"# TYPE для имени ровно один")
	assert.Equal(t, 1, countValueLines(body, "brooq_jobs_total"),
		"строка значения для имени ровно одна")
}

func TestMiddlewareDefaultStatus(t *testing.T) {
	r := New()
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	srv := httptest.NewServer(r.Middleware(inner))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	require.NoError(t, err)
	_ = resp.Body.Close()
	body := bodyMetrics(t, r)
	assert.Contains(t, body, `brooq_http_requests_total{method="GET",status="200"} 1`)
}
