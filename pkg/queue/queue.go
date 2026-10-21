package queue

import (
	"context"
	"time"
)

// Topic — вид задания; по теме выбирается обработчик (маршрутизация).
type Topic string

// Payload — тело задания: полезная нагрузка, схему которой знает только
// потребитель. В базе лежит как JSONB, поэтому байты должны быть
// валидным JSON.
type Payload []byte

// State — положение задания в жизненном цикле (lowercase).
type State string

// Состояния задания. Значения — lowercase, как в SQL-схеме; они же
// записаны в CHECK-ограничении таблицы job.
const (
	// StatePending — ждёт взятия; возможно, до available_at.
	StatePending State = "pending"
	// StateProcessing — взято и закреплено арендой.
	StateProcessing State = "processing"
	// StateFailed — ожидание повтора: сбой записан, попытки не
	// исчерпаны, задание ждёт available_at.
	StateFailed State = "failed"
	// StateDone — исполнено успешно; терминальное.
	StateDone State = "done"
	// StateDead — попытки исчерпаны; терминальное.
	StateDead State = "dead"
	// StateCanceled — отменено; терминальное. Пишется без второй «l»,
	// как context.Canceled.
	StateCanceled State = "canceled"
)

// Job — задание, которое производитель кладёт в очередь.
type Job struct {
	// Topic — тема задания (маршрутизация в обработчик).
	Topic Topic
	// Payload — тело задания.
	Payload Payload
	// IdempotencyKey — ключ конкретной попытки доставки: повторная
	// постановка с тем же ключом не создаёт второе задание.
	IdempotencyKey string
	// AvailableAt — раньше этого времени задание не берётся.
	// Нулевое значение означает «сейчас».
	AvailableAt time.Time
}

// Lease — срок, на который задание закреплено за воркером.
// Инвариант: задание в processing всегда имеет аренду — воркер и срок
// заполнены одновременно.
type Lease struct {
	// Worker — идентификатор воркера, держащего аренду.
	Worker string
	// Until — момент истечения аренды; после него задание свободно.
	Until time.Time
}

// Claim — взятое задание, которое видит обработчик. Обработчик получает
// именно Claim, а не Job: номер попытки и колбэк известны только после
// взятия.
type Claim struct {
	// ID — идентификатор задания в текстовом виде (UUID).
	ID string
	// Topic — тема задания.
	Topic Topic
	// Payload — тело задания.
	Payload Payload
	// Attempt — номер состоявшейся попытки, начиная с 1.
	Attempt int
	// Lease — действующая аренда задания.
	Lease Lease
	// OnSucceeded — колбэк, который ставит следующее задание в той же
	// транзакции, которой закрывается текущее. Может быть nil.
	OnSucceeded func(ctx context.Context, tx Tx) error
	// chain — куда Chain записывает следующее задание. Заполняется
	// движком через WithChain, обработчику недоступно.
	chain ChainSetter
}

// ChainSetter — куда Chain записывает задание. Устанавливает движок.
type ChainSetter func(Job)

// WithChain возвращает копию Claim, у которой Chain(job) вызывает set.
// Вызывается движком при подготовке попытки; обработчику не нужен.
func (c Claim) WithChain(set ChainSetter) Claim {
	c.chain = set
	return c
}

// Chain сообщает движку, какое задание поставить следом в транзакции
// закрытия текущего (D9). Вызывается из обработчика; если движок не
// поддерживает цепочку — no-op.
func (c Claim) Chain(next Job) {
	if c.chain == nil {
		return
	}
	c.chain(next)
}

// ReapedJob — задание, возвращённое в оборот после истечения аренды
// (D8). Тип общий для хранилища и движка: SQL возвращает его, движок
// пишет по нему журнал, и ни одному из слоёв не нужен тип другого.
type ReapedJob struct {
	// ID — идентификатор задания в текстовом виде (UUID).
	ID string
	// Topic — тема задания.
	Topic Topic
}

// DeletedJob — задание, удалённое уборкой терминальных строк; нужно
// только для журнала и метрики.
type DeletedJob struct {
	// ID — идентификатор удалённого задания в текстовом виде (UUID).
	ID string
	// Topic — тема задания.
	Topic Topic
}

// TopicStateCount — число заданий одной темы в одном состоянии. Это
// результат чтения для метрик (D11), а не часть контракта заданий:
// хранилище отдаёт срез таких пар, движок превращает их в gauge
// brooq_jobs{topic,state}.
type TopicStateCount struct {
	// Topic — тема задания.
	Topic Topic
	// State — состояние задания.
	State State
	// Count — число заданий этой темы в этом состоянии.
	Count int64
}

// TopicAge — возраст старейшего готового задания темы: сколько времени
// самое давнее готовое задание уже ждёт взятия. Это результат чтения для
// метрики brooq_ready_oldest_age_seconds (D11), а не часть контракта
// заданий. Темы без готовых заданий в результате отсутствуют.
type TopicAge struct {
	// Topic — тема задания.
	Topic Topic
	// Age — сколько ждёт старейшее готовое задание темы.
	Age time.Duration
}

// KindOptions — настройки темы, регистрируемые вместе с обработчиком.
// Задаются в коде, а не колонкой таблицы: нет миграции и нет расхождения
// «настройка против данных в строке».
type KindOptions struct {
	// MaxAttempts — потолок попыток: после него задание становится
	// мёртвым. Значение меньше 1 — ошибка регистрации.
	MaxAttempts int
	// AttemptTimeout — сколько даётся одной попытке; истечение —
	// сбой попытки с повтором.
	AttemptTimeout time.Duration
	// MaxExecutionTimeout — жёсткий потолок исполнения темы, гасящий
	// контекст безусловно, даже если обработчик жив.
	MaxExecutionTimeout time.Duration
	// BackoffBase — базовый интервал отсрочки до следующей попытки.
	BackoffBase time.Duration
	// BackoffMax — верхняя граница интервала отсрочки.
	BackoffMax time.Duration
}
