# LSM-tree from scratch

Серия статей о том, как устроены LSM-деревья, и полная реализация на Go.

## Блог

Сайт: **https://edylcrm.github.io/db-from-scratch**

Статьи находятся в `_posts/`. Сайт собирается автоматически через GitHub Pages + Jekyll.

## Код

Реализация db находится в директории [`src/`](src/).

```bash
cd src
go test ./...
```

## Как запустить сайт локально

```bash
gem install bundler jekyll
bundle exec jekyll serve
# открыть http://localhost:4000/lsm-from-scratch/
```
