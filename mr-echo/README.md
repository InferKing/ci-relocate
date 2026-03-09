# mr-echo

FastAPI-сервис, который проверяет открытые Merge Request в GitLab и отправляет уведомления в Discord, если MR давно не обновлялся.

## Что делает

- опрашивает список проектов из `GITLAB_PROJECTS`
- ищет открытые MR, у которых `updated_at` старше `STALE_AFTER_HOURS`
- пропускает draft/WIP MR, если `NOTIFY_DRAFT_MRS=0`
- запоминает уже отправленные уведомления в `bot-state.json`
- умеет работать по расписанию и по ручному HTTP-триггеру

## Переменные окружения

Заполни локальный файл [.env](/d:/Projects/mr-echo/.env#L1).

Обязательные:

- `DISCORD_WEBHOOK_URL`
- `GITLAB_TOKEN`
- `GITLAB_PROJECTS`

## Запуск

```powershell
pip install -r requirements.txt
python app.py
```

По умолчанию сервис слушает `0.0.0.0:8000`.

## HTTP endpoints

- `GET /health` - проверка, что процесс жив
- `POST /check` - принудительная проверка stale MR

Если задан `TRIGGER_TOKEN`, для `POST /check` нужно передать заголовок `X-Trigger-Token`.
