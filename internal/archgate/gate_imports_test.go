// Гейт-тесты структуры пакетов: проверка идёт по фактическим
// импортам из `go list`, а не по договорённости в документации. Запреты —
// только на прямые импорты: транзитивная тяга через легальный слой
// (например, pgx приходит в движок через pkg/postgres) — не
// нарушение.
package archgate

import (
	"os/exec"
	"strings"
	"testing"
)

const module = "github.com/thefish/brooq"

// shared — переиспользуемые пакеты (pkg/*): конфиг, пул, логгер, словарь
// очереди, SQL-адаптер. Они не знают ни домена, ни внутренних пакетов —
// иначе их нельзя было бы подключить снаружи (в тестах, в соседнем юните)
// без тяги всего ядра. pkg/postgres здесь ради внешнего адреса адаптера;
// pgx ему разрешён (прецедент — pkg/db).
var shared = []string{
	module + "/pkg/config",
	module + "/pkg/db",
	module + "/pkg/logger",
	module + "/pkg/postgres",
	module + "/pkg/queue",
}

// engine — пакет движка очереди: он оркестрирует, но сам не знает ни
// драйвера БД, ни транспорта. Движок переехал из internal/queue в
// публичный pkg/brooq (D9): потребитель подключает его снаружи модуля,
// а запрет на драйвер и транспорт остаётся тот же. SQL живёт в
// pkg/postgres, поэтому прямой импорт pgx или net/http
// здесь — утечка инфраструктуры в оркестрацию.
var engine = module + "/pkg/brooq"

// forbiddenInEngine — то, чего в движке быть не может напрямую: драйвер
// БД и пакеты транспорта. Транзитивно pgx приходит через
// pkg/postgres, и это законно: проверяются только прямые импорты
// (go list без -deps).
var forbiddenInEngine = []string{
	"github.com/jackc/pgx",
	"net/http",
}

// TestSharedPackagesDoNotImportInternal — pkg/* не тянет internal/*.
func TestSharedPackagesDoNotImportInternal(t *testing.T) {
	for _, pkg := range shared {
		pkg := pkg
		t.Run(strings.TrimPrefix(pkg, module+"/"), func(t *testing.T) {
			deps, exists := goListDeps(pkg)
			if !exists {
				t.Fatalf("пакет %s должен существовать (общий слой харнесса)", pkg)
			}
			if strings.Contains(deps, module+"/internal/") {
				t.Errorf("общий пакет %s не должен импортировать internal/*", pkg)
			}
		})
	}
}

// TestEngineDoesNotImportInfrastructure — движок очереди не импортирует
// напрямую драйвер БД и HTTP. Пакет уже существует, поэтому пропуска
// (t.Skip) здесь нет: гейт проверяет факт, а не обещание.
func TestEngineDoesNotImportInfrastructure(t *testing.T) {
	imports, exists := goListImports(engine)
	if !exists {
		t.Fatalf("пакет %s должен существовать: движок очереди реализован", engine)
	}
	for _, bad := range forbiddenInEngine {
		if strings.Contains(imports, bad) {
			t.Errorf("движок %s не должен импортировать %s напрямую: SQL живёт в pkg/postgres",
				engine, bad)
		}
	}
}

// goListDeps возвращает вывод `go list -deps <pkg>` (по строке на пакет)
// и признак существования пакета (false = нет каталога/модуля).
func goListDeps(pkg string) (string, bool) {
	out, err := exec.Command("go", "list", "-deps", pkg).Output() //nolint:gosec // G204: pkg — фиксированный список пакетов теста, не пользовательский ввод
	if err != nil {
		return "", false
	}
	return string(out), true
}

// goListImports возвращает прямые импорты пакета
// (`go list -f '{{.Imports}}'`) и признак существования пакета. Именно
// прямые, без -deps: транзитивные зависимости подконтрольны не этому
// гейту, и запрет на них запретил бы легальный слой repository.
func goListImports(pkg string) (string, bool) {
	out, err := exec.Command("go", "list", "-f", "{{.Imports}}", pkg).Output() //nolint:gosec // G204: pkg — фиксированный список пакетов теста, не пользовательский ввод
	if err != nil {
		return "", false
	}
	return string(out), true
}

// public — публичные пакеты, которые обязаны оставаться свободными от
// инфраструктуры. pkg/db сюда не входит: он по определению обёртка над
// драйвером. pkg/brooq — движок: он публичный (его
// импортирует потребитель) и работает с портом Repo, поэтому драйвер
// и транспорт ему не нужны.
var public = []string{
	module + "/pkg/brooq",
	module + "/pkg/config",
	module + "/pkg/logger",
	module + "/pkg/queue",
}

// forbiddenInPublic — то, чего в публичном пакете быть не может:
// внутренние пакеты модуля, драйвер БД и транспорт.
var forbiddenInPublic = []string{
	module + "/internal/",
	"github.com/jackc/pgx",
	"net/http",
}

// TestPublicPackagesDoNotImportForbidden — публичный пакет не тянет
// internal/*, pgx и net/http: словарь очереди и общий слой подключаются
// снаружи без ядра, драйвера и HTTP.
func TestPublicPackagesDoNotImportForbidden(t *testing.T) {
	for _, pkg := range public {
		t.Run(strings.TrimPrefix(pkg, module+"/"), func(t *testing.T) {
			deps, exists := goListDeps(pkg)
			if !exists {
				t.Fatalf("пакет %s должен существовать", pkg)
			}
			for _, bad := range forbiddenInPublic {
				if strings.Contains(deps, bad) {
					t.Errorf("публичный пакет %s не должен импортировать %s", pkg, bad)
				}
			}
		})
	}
}
