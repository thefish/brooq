// Package observability — базовая наблюдаемость юнита: middleware
// латентности/ошибок и прометеусовский /metrics (text/plain 0.0.4, без
// внешних зависимостей). Счётчики и gauge очереди (взято/подтверждено/
// повтор/мёртвое, глубина и возраст) регистрирует движок — здесь только
// реестр и рендер.
//
// Пакет «анемный»: домен и repository он не импортирует, поэтому его
// вправе подключать и воркер, и служебная поверхность.
package observability

import (
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// label — пара имя=значение метрики (Prometheus-лейбл).
type label struct {
	name  string
	value string
}

// series — одна временная серия реестра: значения одного имени с одним
// набором лейблов. Тип серии здесь не хранится: тип принадлежит имени
// (metric.kind), потому что Prometheus запрещает одно имя с двумя типами.
// Значение хранится как float64: text-формат Prometheus дробный по своей
// природе, а длительности попыток и возраст старейшего задания измеряются
// долями секунды. Целые значения печатаются как `2`, дробные как `0.25`.
type series struct {
	name   string
	labels []label
	value  float64
}

// metric — метаданные имени метрики: HELP и тип, зафиксированный при
// первом обращении к имени (Inc/Add → counter, Set → gauge).
type metric struct {
	name string
	help string
	kind string
}

const (
	// kindCounter — монотонно растущий счётчик событий (Add/Inc).
	kindCounter = "counter"
	// kindGauge — мгновенное значение, которое перезаписывается (Set).
	kindGauge = "gauge"
)

// Registry — реестр счётчиков и gauge юнита: потокобезопасный,
// рендерится в Prometheus text-формат обработчиком /metrics.
//
// Ключ серии — `имя{лейблы}`, без типа: одно имя метрики имеет ровно один
// тип, поэтому в выводе не может появиться двух # TYPE с одним именем.
type Registry struct {
	mu     sync.Mutex
	series map[string]*series
	meta   map[string]*metric // метаданные имени (HELP и тип)
	keys   []string           // стабильный порядок ключей для вывода
}

// New собирает пустой реестр.
func New() *Registry {
	return &Registry{series: map[string]*series{}, meta: map[string]*metric{}}
}

// fixKind фиксирует тип имени при первом обращении и сверяет его с
// последующими. Возвращает false, если обращение конфликтует с уже
// зафиксированным типом: тогда вызывающий обязан проигнорировать вызов
// (значение не меняется, вторая серия не создаётся, # TYPE остаётся один).
//
// Конфликт типа — ошибка программиста (Set по счётчику или Add по gauge),
// но паниковать нельзя: реестр обслуживает /metrics, и падение обработчика
// уронило бы наблюдаемость. Сигнатуры Inc/Add/Set менять нельзя — порт
// метрик (pkg/brooq) и его вызовы не обрабатывают ошибок, поэтому о
// конфликте сообщается в лог (slog.Warn), а вызов игнорируется.
func (r *Registry) fixKind(name, help, kind string) bool {
	m, ok := r.meta[name]
	if !ok {
		if help == "" {
			help = name
		}
		r.meta[name] = &metric{name: name, help: help, kind: kind}
		return true
	}
	if m.kind != kind {
		slog.Warn("observability: конфликт типа метрики — вызов проигнорирован",
			"metric", name, "fixed_kind", m.kind, "call_kind", kind)
		return false
	}
	return true
}

// seriesKey собирает ключ серии `имя{лейблы}` — без типа в ключе.
func seriesKey(name string, batch []label) string {
	return name + "{" + labelKey(batch) + "}"
}

// getSeries возвращает серию по ключу, создавая её при первом появлении.
func (r *Registry) getSeries(key, name string, batch []label) *series {
	s, ok := r.series[key]
	if !ok {
		s = &series{name: name, labels: batch}
		r.series[key] = s
		r.keys = append(r.keys, key)
		sort.Strings(r.keys)
	}
	return s
}

// Add увеличивает счётчик name на delta (обычно 1) с лейблами.
// help — первая установка имени запоминает HELP. Порядок лейблов
// приводится к лексикографическому (детерминированный вывод).
//
// Тип имени фиксируется при первом обращении (counter). Если имя уже
// занято gauge (Set), вызов игнорируется — см. fixKind.
func (r *Registry) Add(name, help string, labels map[string]string, delta uint64) {
	r.AddFloat(name, help, labels, float64(delta))
}

// AddFloat увеличивает счётчик name на дробную delta (сумма длительностей
// попыток в секундах): доли секунды не теряются. Тип имени, HELP и порядок
// лейблов — как у Add: имя, зафиксированное как gauge (Set), игнорируется.
func (r *Registry) AddFloat(name, help string, labels map[string]string, delta float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.fixKind(name, help, kindCounter) {
		return
	}
	batch := sortedLabels(labels)
	r.getSeries(seriesKey(name, batch), name, batch).value += delta
}

// Set устанавливает gauge name в абсолютное значение value с лейблами.
//
// Gauge отличается от счётчика тем, что показывает «сколько сейчас», а
// не «сколько всего было»: глубину очереди, занятость воркеров, возраст
// старейшего задания. Поэтому повторный вызов Set перезаписывает
// значение, а не суммирует его: сложение двух измерений дало бы
// бессмысленное число (и росло бы без остановки).
//
// HELP запоминает первая установка имени. Порядок лейблов приводится к
// лексикографическому. Тип имени фиксируется при первом обращении
// (gauge): Set по имени, уже занятому счётчиком (Add/Inc), не подменяет
// тип — такой вызов игнорируется (см. fixKind), поэтому счётчик остаётся
// счётчиком, а второго # TYPE с тем же именем не появляется.
//
// Значение дробное: возраст старейшего задания и занятость пула бывают
// долями секунды, а целые значения (глубина очереди) печатаются как `2`.
func (r *Registry) Set(name, help string, labels map[string]string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.fixKind(name, help, kindGauge) {
		return
	}
	batch := sortedLabels(labels)
	r.getSeries(seriesKey(name, batch), name, batch).value = value
}

// sortedLabels приводит лейблы к лексикографическому порядку
// (детерминированный вывод и стабильный ключ серии).
func sortedLabels(labels map[string]string) []label {
	batch := make([]label, 0, len(labels))
	for k, v := range labels {
		batch = append(batch, label{k, v})
	}
	sort.Slice(batch, func(i, j int) bool { return batch[i].name < batch[j].name })
	return batch
}

// Inc — Add с delta=1.
func (r *Registry) Inc(name, help string, labels map[string]string) {
	r.Add(name, help, labels, 1)
}

// labelKey серриализует лейблы в prometheus-виде (`k="v",k2="v2"`).
func labelKey(batch []label) string {
	var sb strings.Builder
	for i, l := range batch {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(l.name)
		sb.WriteString(`="`)
		sb.WriteString(escapeLabel(l.value))
		sb.WriteString(`"`)
	}
	return sb.String()
}

// escapeLabel экранирует кавычки/обратный слэш в значении лейбла.
func escapeLabel(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	return strings.ReplaceAll(v, `"`, `\"`)
}

// Handler — GET /metrics: прометеусовский текст (text/plain; version=0.0.4).
// # HELP и # TYPE печатаются ровно один раз на имя метрики (серии одного
// имени идут подряд — ключи отсортированы), тип берётся из метаданных
// имени. Порядок серий стабилен. Формат ответа и Content-Type не менялись.
func (r *Registry) Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	r.mu.Lock()
	defer r.mu.Unlock()
	printed := "" // имя, для которого HELP/TYPE уже напечатаны
	for _, key := range r.keys {
		s := r.series[key]
		if s.name != printed {
			m := r.meta[s.name]
			_, _ = fmt.Fprintf(w, "# HELP %s %s\n", m.name, m.help)
			_, _ = fmt.Fprintf(w, "# TYPE %s %s\n", m.name, m.kind)
			printed = s.name
		}
		// Формат 'g' с точностью -1: минимальное представление, 2 для
		// целых и 0.25 для дробных значений.
		v := strconv.FormatFloat(s.value, 'g', -1, 64)
		if len(s.labels) == 0 {
			_, _ = fmt.Fprintf(w, "%s %s\n", s.name, v)
		} else {
			_, _ = fmt.Fprintf(w, "%s{%s} %s\n", s.name, labelKey(s.labels), v)
		}
	}
}

// Middleware — латентность и код ответа каждого запроса: в
// http_requests_total{method,status} и summary-пару
// http_request_duration_seconds_{sum,count}{method} (текстовый формат без
// гистограмм: сумма+счётчик скрейпятся как summary).
func (r *Registry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, req)
		method := req.Method
		status := strconv.Itoa(rec.status)
		// G115: time.Since неотрицательна; max-гейт делает конверсию безопасной.
		ms := time.Since(start).Milliseconds()
		if ms < 0 {
			ms = 0
		}
		dur := uint64(ms)
		r.Add("brooq_http_requests_total", "HTTP-запросы юнита",
			map[string]string{"method": method, "status": status}, 1)
		r.Add("brooq_http_request_duration_ms_sum", "Сумма длительностей HTTP-запросов, мс",
			map[string]string{"method": method}, dur)
		r.Add("brooq_http_request_duration_ms_count", "Число HTTP-запросов по методу",
			map[string]string{"method": method}, 1)
	})
}

// statusRecorder запоминает код ответа для middleware-метрик.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader фиксирует код (первый вызов).
func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
