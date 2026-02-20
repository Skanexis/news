# Деплой проекта на Ubuntu VPS

Для IONOS с детальными шагами DNS/Firewall/Public IP используйте: `DEPLOY_IONOS_VPS.md`.

## 1. Что важно про текущий проект

- `frontend/index.html` — статический фронтенд (без сборки).
- `backend/index.js` — Node.js/Express API.
- Данные хранятся локально в `backend/db.sqlite` и `backend/uploads/`.
- Планировщик (`cron` + `setInterval`) работает внутри процесса backend.
- API не имеет встроенной авторизации (в проде обязательно ограничить доступ через Nginx).

Критично: запускайте только **1 инстанс backend**, иначе возможны дубли отправок (из-за встроенного планировщика в каждом процессе).

---

## 2. Подготовка Ubuntu VPS

Пример для Ubuntu 22.04/24.04.

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl ca-certificates nginx ufw build-essential python3 make g++ apache2-utils
```

Установите Node.js (рекомендуется 22.x; также подходит 20.x).  
`better-sqlite3` в проекте требует Node 20+.

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install -y nodejs
node -v
npm -v
```

Откройте порты:

```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
sudo ufw status
```

---

## 3. Размещение проекта на сервере

```bash
sudo mkdir -p /opt/yosupport
sudo chown -R $USER:$USER /opt/yosupport
cd /opt/yosupport
```

Вариант A (git):

```bash
git clone <REPO_URL> app
cd /opt/yosupport/app
```

Вариант B (архив/rsync): распакуйте проект в `/opt/yosupport/app`.

Если переносите проект с Windows, обязательно переустановите зависимости на VPS:

```bash
cd /opt/yosupport/app/backend
rm -rf node_modules
npm ci --omit=dev
```

Если хотите перенести существующие данные (компании, посты, логи, черновики медиа), скопируйте:
- `backend/db.sqlite`
- `backend/db.sqlite-wal`
- `backend/db.sqlite-shm`
- `backend/uploads/`

Лучше делать копию при остановленном backend.

---

## 4. Настройка `.env`

```bash
cd /opt/yosupport/app/backend
cp .env.example .env
nano .env
```

Минимально заполните:

```env
TELEGRAM_BOT_TOKEN=@8374815692:AAFW_3HQDDpLdZTClo9hirThx3yjohgF568
TELEGRAM_CHANNEL_ID=@channel_or_chat_id
TELEGRAM_WEBHOOK_SECRET=long_random_secret
PUBLIC_BASE_URL=https://admin.yosupport.it
CRON_TIME=0 9 * * *
CRON_TZ=Europe/Moscow
ANALYTICS_SALT=long_random_salt
PORT=3000
CORS_ORIGIN=https://admin.yosupport.it
ENABLE_SECURITY_HEADERS=1
```

Примечание: переменные `IG_*`, если есть в вашем локальном `.env`, в текущем коде не используются.

---

## 5. API frontend в production

Ручная правка `frontend/index.html` больше не нужна:
- в режиме `https://your-domain.com` frontend автоматически использует `/api`;
- при локальном открытии файла или запуске на `localhost:3000` используется прямой backend `http://localhost:3000`.

При необходимости можно принудительно указать API:
- query-параметр: `?api=https://api.example.com`
- или через `localStorage` ключ `yosupport_api_base`.

---

## 6. Запуск backend как systemd-сервис

Проверьте путь до node:

```bash
which node
```

Создайте сервис (или используйте шаблон из репозитория `deploy/systemd/yosupport-backend.service`):

```bash
sudo nano /etc/systemd/system/yosupport-backend.service
```

Содержимое:

```ini
[Unit]
Description=YoSupport Backend
After=network.target

[Service]
Type=simple
User=<YOUR_LINUX_USER>
WorkingDirectory=/opt/yosupport/app/backend
ExecStart=/usr/bin/node index.js
Restart=always
RestartSec=5
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
```

Замените `<YOUR_LINUX_USER>` на пользователя, от которого вы деплоите проект (обычно тот же, под кем делали `git clone`).
Если `which node` показывает путь отличный от `/usr/bin/node`, замените `ExecStart` на корректный путь.

Запуск:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now yosupport-backend
sudo systemctl status yosupport-backend
curl http://127.0.0.1:3000/health
```

Логи:

```bash
journalctl -u yosupport-backend -f
```

---

## 7. Nginx: статика + reverse proxy + базовая защита

### 7.1. Basic Auth для админки (рекомендуется)

API проекта не защищен авторизацией, поэтому закройте хотя бы `/api` через basic auth.
UI (`/`) лучше оставить без `auth_basic`, чтобы на телефонах и в Telegram WebView открывалась встроенная форма логина на странице, а не голый `401`.

```bash
sudo htpasswd -c /etc/nginx/.htpasswd_yosupport admin
```

### 7.2. Конфиг Nginx

```bash
sudo nano /etc/nginx/sites-available/yosupport
```

```nginx
server {
    listen 80;
    listen [::]:80;
    server_name your-domain.com;

    root /opt/yosupport/app/frontend;
    index index.html;

    # UI без Basic Auth (чтобы корректно открывался на мобильных/в Telegram)
    location / {
        try_files $uri $uri/ /index.html;
    }

    # API для frontend: /api/* -> backend /*
    location /api/ {
        auth_basic "Restricted API";
        auth_basic_user_file /etc/nginx/.htpasswd_yosupport;

        proxy_pass http://127.0.0.1:3000/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Публичные редирект-ссылки аналитики
    location /r/ {
        auth_basic off;
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Telegram webhook должен быть публичным
    location = /telegram/webhook {
        auth_basic off;
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Можно взять готовый шаблон из репозитория: `deploy/nginx/yosupport.conf`.

Включите сайт:

```bash
sudo ln -s /etc/nginx/sites-available/yosupport /etc/nginx/sites-enabled/yosupport
sudo nginx -t
sudo systemctl reload nginx
```

---

## 8. HTTPS (Let's Encrypt)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
sudo systemctl status certbot.timer
```

После этого проверьте:

```bash
curl -I https://your-domain.com
```

---

## 9. Telegram webhook (рекомендуемый режим)

Если используете webhook, настройте его:

```bash
curl -sS "https://api.telegram.org/bot<>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://your-domain.com/telegram/webhook","secret_token":"<WEBHOOK_SECRET>","allowed_updates":["message","channel_post"]}'
```

Или используйте готовый скрипт из проекта:

```bash
cd /opt/yosupport/app
chmod +x deploy/scripts/set-telegram-webhook.sh
./deploy/scripts/set-telegram-webhook.sh "<BOT_TOKEN>" "https://your-domain.com" "<WEBHOOK_SECRET>"
```

Важно: при активном webhook Telegram не дает читать обновления через `getUpdates`, поэтому ручная синхронизация через `/telegram/pull` будет конфликтовать.

---

## 10. Проверка после деплоя

1. Backend локально:

```bash
curl http://127.0.0.1:3000/health
```

2. Через домен (под basic auth):

```bash
curl -u admin:<password> https://your-domain.com/api/health
```

3. Откройте `https://your-domain.com`, авторизуйтесь, проверьте:
- создание компании,
- создание поста,
- генерацию расписания,
- отправку в Telegram.

---

## 11. Бэкап и восстановление данных

Бэкап (лучше при остановленном backend):

```bash
sudo systemctl stop yosupport-backend
sudo mkdir -p /opt/backups
sudo tar -czf /opt/backups/yosupport-$(date +%F-%H%M).tar.gz \
  /opt/yosupport/app/backend/db.sqlite \
  /opt/yosupport/app/backend/db.sqlite-wal \
  /opt/yosupport/app/backend/db.sqlite-shm \
  /opt/yosupport/app/backend/uploads \
  /opt/yosupport/app/backend/.env
sudo systemctl start yosupport-backend
```

Восстановление:

```bash
sudo systemctl stop yosupport-backend
sudo tar -xzf /opt/backups/<backup-file>.tar.gz -C /
sudo systemctl start yosupport-backend
```

---

## 12. Обновление приложения

```bash
cd /opt/yosupport/app
git pull
cd backend
npm ci --omit=dev
sudo systemctl restart yosupport-backend
sudo systemctl status yosupport-backend
```

После обновления проверьте `https://your-domaiTn.com/api/health` и UI.

---

## 13. Частые проблемы

1. Frontend не видит API.  
Проверьте, что в `frontend/index.html` стоит `const API = '/api';`.

2. Ошибка `better-sqlite3` при установке.  
Проверьте Node версии 20+ и наличие `build-essential`, `python3`, `make`, `g++`.

3. Дубли отправок в Telegram.  
Убедитесь, что запущен только один процесс backend.

4. Не приходят webhook-обновления.  
Проверьте `TELEGRAM_WEBHOOK_SECRET`, путь `/telegram/webhook`, HTTPS и ответ Telegram API после `setWebhook`.

5. На телефоне/в Telegram сразу `401 Unauthorized` без окна логина.  
Так происходит из-за `auth_basic` на `location /` (встроенный браузер Telegram часто не показывает basic auth диалог).  
Решение: уберите `auth_basic` с `/`, оставьте его только на `/api/`, затем `sudo nginx -t && sudo systemctl reload nginx`.
