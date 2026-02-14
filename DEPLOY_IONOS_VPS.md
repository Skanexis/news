# Полный деплой на IONOS VPS (Ubuntu) + привязка домена

Инструкция ниже заточена под ваш проект из этого репозитория:
- backend: `backend/index.js` (Node.js + SQLite + cron)
- frontend: `frontend/index.html` (статика)

## 0. Что нужно заранее

1. Домен в IONOS (или перенаправленный в IONOS DNS).
2. VPS с Ubuntu в IONOS.
3. Публичный IPv4 у сервера (в IONOS Cloud Panel).
4. SSH-доступ к серверу.

Примечание: в вашем проекте планировщик работает внутри backend-процесса, поэтому в проде запускайте только 1 инстанс backend.

---

## 1. Проверить/назначить Public IP в IONOS

В IONOS:
1. `Menu -> Server & Cloud`
2. `Network -> Public IP`
3. Убедитесь, что у вашего VPS есть назначенный Public IPv4.

Если IP нет:
1. Нажмите `Create`
2. Назначьте IP вашему серверу
3. Сохраните.

Опционально: сразу задайте Reverse DNS (PTR) для IP (полезно для почтовых сценариев).

---

## 2. Проверить Cloud Firewall в IONOS

В IONOS:
1. `Menu -> Server & Cloud`
2. `Network -> Firewall Policies`
3. Проверьте, что для VPS открыты входящие порты:
- `22/tcp` (SSH)
- `80/tcp` (HTTP)
- `443/tcp` (HTTPS)

Если не открыты, добавьте правила и сохраните policy.

---

## 3. Привязать домен к VPS через DNS в IONOS

### 3.1. Важная проверка перед изменением

Откройте домен в IONOS:
1. `Domains & SSL`
2. У нужного домена: `Actions -> DNS`

Проверьте NS:
- Если используются IONOS nameservers, настраивайте DNS прямо здесь.
- Если выставлены custom nameservers, менять записи нужно у того DNS-провайдера, который указан в NS.

### 3.2. Базовый вариант (домен и www на один VPS)

В DNS-зоне домена:
1. Удалите/деактивируйте конфликтующие `A/AAAA` записи для корня (`@` или пустой host), если они смотрят не на ваш VPS.
2. Создайте/обновите:
- `A` запись: `Host @` (или пусто) -> `YOUR_VPS_IPV4`
- `A` запись: `Host www` -> `YOUR_VPS_IPV4`

Опционально:
- если у вас есть IPv6 и он реально настроен на сервере/Nginx, добавьте `AAAA` для `@` и `www`;
- если IPv6 не используете, не оставляйте старые `AAAA`, иначе часть пользователей может ходить мимо вашего VPS.

Важно: MX/TXT для почты не трогайте, если не планируете перенос почты.

### 3.3. Если нужен только поддомен (например `app.example.com`)

Добавьте:
- `A` запись: `Host app` -> `YOUR_VPS_IPV4`

Корневой домен (`@`) можно оставить на старом сайте.

### 3.4. Сколько ждать

В IONOS изменения обычно применяются быстро, но внешняя видимость обычно до 1 часа, в редких случаях до 72 часов.

---

## 4. Проверка DNS до деплоя

Локально (на вашем ПК) проверьте:

```bash
nslookup your-domain.com
nslookup www.your-domain.com
```

На самом сервере (после SSH):

```bash
getent hosts your-domain.com
getent hosts www.your-domain.com
```

Оба имени должны резолвиться в ваш VPS IP.

---

## 5. Подготовить Ubuntu на VPS

Подключение:

```bash
ssh root@YOUR_VPS_IPV4
```

Создайте пользователя (рекомендуется):

```bash
adduser deploy
usermod -aG sudo deploy
```

Дальше под `deploy`:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl ca-certificates nginx ufw build-essential python3 make g++ apache2-utils
```

Node.js 22:

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
sudo apt install -y nodejs
node -v
npm -v
```

UFW:

```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw --force enable
sudo ufw status
```

---

## 6. Залить проект на VPS

```bash
sudo mkdir -p /opt/yosupport
sudo chown -R $USER:$USER /opt/yosupport
cd /opt/yosupport
git clone <YOUR_REPO_URL> app
cd /opt/yosupport/app/backend
npm ci --omit=dev
```

Если переносите прод-данные со старой машины, скопируйте:
- `backend/db.sqlite`
- `backend/db.sqlite-wal`
- `backend/db.sqlite-shm`
- `backend/uploads/`

---

## 7. Настроить `.env`

```bash
cd /opt/yosupport/app/backend
cp .env.example .env
nano .env
```

Пример:

```env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHANNEL_ID=@channel_or_chat_id
TELEGRAM_WEBHOOK_SECRET=very_long_random_secret
PUBLIC_BASE_URL=https://your-domain.com
CRON_TIME=0 9 * * *
CRON_TZ=Europe/Moscow
ANALYTICS_SALT=very_long_random_salt
PORT=3000
CORS_ORIGIN=https://your-domain.com,https://www.your-domain.com
ENABLE_SECURITY_HEADERS=1
```

---

## 8. Запуск backend через systemd

В репозитории уже есть шаблон: `deploy/systemd/yosupport-backend.service`.

Установите:

```bash
cd /opt/yosupport/app
sudo cp deploy/systemd/yosupport-backend.service /etc/systemd/system/yosupport-backend.service
sudo nano /etc/systemd/system/yosupport-backend.service
```

Проверьте и замените:
- `User=<YOUR_LINUX_USER>` -> `deploy` (или ваш пользователь)
- `ExecStart=/usr/bin/node index.js` -> путь из `which node`, если отличается

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

## 9. Nginx на домен (IONOS DNS уже должен указывать на VPS)

В репозитории есть шаблон: `deploy/nginx/yosupport.conf`.

Установите:

```bash
cd /opt/yosupport/app
sudo cp deploy/nginx/yosupport.conf /etc/nginx/sites-available/yosupport
sudo nano /etc/nginx/sites-available/yosupport
```

Измените:
- `server_name your-domain.com;` -> ваш домен

Включите сайт:

```bash
sudo ln -s /etc/nginx/sites-available/yosupport /etc/nginx/sites-enabled/yosupport
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx
```

Если используете Basic Auth (рекомендуется):

```bash
sudo htpasswd -c /etc/nginx/.htpasswd_yosupport admin
sudo systemctl reload nginx
```

Проверка:

```bash
curl -I http://your-domain.com
curl -I http://your-domain.com/api/health
```

---

## 10. SSL (Let's Encrypt)

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com -d www.your-domain.com
sudo systemctl status certbot.timer
```

Проверка:

```bash
curl -I https://your-domain.com
curl -I https://www.your-domain.com
```

---

## 11. Telegram webhook

Используйте скрипт из репозитория:

```bash
cd /opt/yosupport/app
chmod +x deploy/scripts/set-telegram-webhook.sh
./deploy/scripts/set-telegram-webhook.sh "<BOT_TOKEN>" "https://your-domain.com" "<WEBHOOK_SECRET>"
```

---

## 12. Финальный чеклист запуска

1. DNS:
- `your-domain.com` и `www` -> ваш VPS IP

2. Backend:
- `systemctl status yosupport-backend` = active
- `curl http://127.0.0.1:3000/health` -> `{"ok":true}`

3. Nginx/SSL:
- `https://your-domain.com` открывается
- `https://your-domain.com/api/health` отвечает

4. Приложение:
- вход в UI
- создание компании/поста
- тест публикации
- логирование отправки

---

## 13. Частые проблемы именно на IONOS

1. Открывается не ваш сайт, а старая страница/парковка.  
Обычно остались старые `A/AAAA` записи, указывающие не на VPS.

2. `www` не работает, а корень работает.  
Нет отдельной DNS записи для `www` (или неверная).

3. HTTPS не выпускается через Certbot.  
DNS еще не распространился или порт 80 закрыт в Cloud Firewall/UFW.

4. Сервер недоступен снаружи, хотя сервисы запущены.  
Проверьте policy в `IONOS -> Server & Cloud -> Network -> Firewall Policies`.

5. Вебхук Telegram не прилетает.  
Проверьте публичность `https://your-domain.com/telegram/webhook`, секрет в `.env`, и ответ `setWebhook`.

---

## 14. Официальные материалы IONOS (актуальные)

- DNS A/AAAA:  
  https://www.ionos.com/help/domains/configuring-your-ip-address/changing-a-domains-ipv4/ipv6-address-a/aaaa-record/
- Время применения DNS:  
  https://www.ionos.com/help/domains/general-information-about-dns-settings/time-required-for-dns-changes/
- Использование custom nameservers:  
  https://www.ionos.com/help/domains/using-your-own-name-servers/using-your-own-name-servers-for-a-domain/
- Public IP (VPS):  
  https://www.ionos.com/help/server-cloud-infrastructure/ip-addresses-vps/creating-a-public-ip-address-vps-and-migrated-cloud-servers/
- Firewall policies (VPS):  
  https://www.ionos.com/help/server-cloud-infrastructure/firewall-vps/editing-your-firewall-policy-vps-and-migrated-cloud-servers/
