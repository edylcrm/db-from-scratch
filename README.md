# Database From Scratch

Пишем базу данных с нуля на Go. Серия статей с кодом, объяснениями и заданиями для самостоятельной реализации.

Сайт: **https://edylcrm.github.io/db-from-scratch**

## О чём этот проект

Цикл построен вокруг написания своей базы данных с нуля — от хранения байтов на диске до выполнения SQL-запросов и распределённого консенсуса.

Цель — не production-ready БД, а глубокое понимание того, как устроены современные базы данных изнутри.
Каждая глава содержит теорию, задание с unit-тестами и реализацию с подробными комментариями.

## Карта тем

**Storage Engine** — фундамент, на котором всё держится: append-only log, hash index, LSM-tree (memtable, WAL, SSTable, compaction).

**Transactions** — MVCC, транзакционный менеджер, изоляция.

**SQL** — лексер, парсер, catalog, query executor.

**Query Optimizer** — статистика, гистограммы, cost-based optimizer.

**Raft** — консенсус для репликации данных между нодами.

## Опубликованные главы

- [Часть 0: О чём этот блог и зачем всё это](https://edylcrm.github.io/db-from-scratch/2026/04/29/part-0-intro.html)
- [Часть 1: Storage Engine — с чего всё начинается](https://edylcrm.github.io/db-from-scratch/2026/04/29/part-1.html)
- [Часть 1.1: Append-Only Log — самое простое хранилище](https://edylcrm.github.io/db-from-scratch/2026/04/30/part-1.1.html)
- [Часть 1.2: Hash Index — ускоряем чтение до O(1)](https://edylcrm.github.io/db-from-scratch/2026/04/30/part-1.2.html)
- [Часть 1.3: Итоги — Append-Only Log и Hash Index](https://edylcrm.github.io/db-from-scratch/2026/05/01/part-1.3.html)
- [Часть 1.4: LSM-tree — как устроен современный storage engine](https://edylcrm.github.io/db-from-scratch/2026/05/02/part-1.4.html)
- [Часть 1.5: Skip List — структура данных для memtable](https://edylcrm.github.io/db-from-scratch/2026/05/02/part-1.5.html)

## Как читать

Каждая глава устроена одинаково:

1. **Теория** — как устроен компонент, зачем нужен, какие есть подходы
2. **Задание** — шаблон кода с TODO и набор unit-тестов для самопроверки
3. **Реализация** — готовый код с подробными комментариями (доступен по тегам: `part-1.1`, `part-1.2`, ...)

Можно попробовать написать всё самому, прогнать тесты и проверить себя. А можно просто читать готовый код как учебный материал.

## Ориентиры

Проекты, на которые опирается этот цикл:

- **[LevelDB](https://github.com/google/leveldb)** — компактная embedded-БД от Google, одна из первых популярных реализаций LSM
- **[RocksDB](https://github.com/facebook/rocksdb)** — форк LevelDB от Meta, стандарт де-факто для LSM-хранилищ
- **[Pebble](https://github.com/cockroachdb/pebble)** — реализация LSM на Go от Cockroach Labs
- **[CockroachDB](https://github.com/cockroachdb/cockroach)** — распределённая SQL-база поверх Pebble

## Структура репозитория

```
_posts/          — статьи блога (Jekyll)
src/storage/     — реализация storage engine на Go
```

## Запуск тестов

```bash
cd src && go test ./storage/ -v
```

## Запуск сайта локально

```bash
gem install bundler jekyll
bundle exec jekyll serve
# открыть http://localhost:4000/db-from-scratch/
```