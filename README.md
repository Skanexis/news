# Pianificatore Annunci Telegram v9

## Deploy Ubuntu VPS
- Guida completa: `DEPLOY_UBUNTU_VPS.md`
- IONOS-specific: `DEPLOY_IONOS_VPS.md`
- Template pronti: `deploy/nginx/yosupport.conf`, `deploy/systemd/yosupport-backend.service`

## Avvio
1. Backend
   - `cd backend`
   - `npm install`
   - copia `.env.example` in `.env`
   - `node index.js`
2. Frontend
   - apri `frontend/index.html` nel browser

## Flusso bozze Telegram
- Crea un canale o gruppo Telegram privato accessibile solo ad admin e bot.
- Aggiungi il bot come admin (canale) o membro (gruppo) per permettere la lettura dei messaggi.
- Opzione A: Webhook (consigliato)
  - Esponi il backend in HTTPS (ngrok o dominio reale).
  - Imposta `PUBLIC_BASE_URL` in `.env` con quell'URL HTTPS.
  - Imposta `TELEGRAM_WEBHOOK_SECRET` in `.env`.
  - Chiama `setWebhook` di Telegram con URL pubblico:
    - `https://api.telegram.org/bot<YOUR_TOKEN>/setWebhook`
    - Body JSON: `{"url":"https://your-public-url/telegram/webhook","secret_token":"your-secret","allowed_updates":["message","channel_post"]}`
- Opzione B: Pull aggiornamenti (senza webhook)
  - Clicca **Sincronizza bozze** nell'interfaccia (chiama `/telegram/pull`).
  - Nota: Telegram non permette `getUpdates` quando un webhook è attivo.

Le bozze acquisite appaiono nel blocco **Bozze Telegram** e possono essere collegate ai post.

## Impostazioni pianificazione (UI admin)
- Primo avvio automatico: inizio della prima sessione giornaliera.
- Secondo avvio automatico (opzionale): inizio della seconda sessione giornaliera.
- Intervallo invio configurabile in minuti (es. 5, 10, 15) tra i post.
- Ogni sessione prova a inviare tutti i post attivi del giorno.
- Nel blocco forecast viene mostrata anche la fine stimata del primo ciclo.

## Analitiche link
- Attiva il tracciamento link nel form del post.
- I link CTA vengono riscritti come `/r/:code` e i click vengono salvati.
- `PUBLIC_BASE_URL` deve essere configurato per rendere raggiungibili i link tracciati.
- `ANALYTICS_SALT` viene usato per hashare gli IP (privacy).

## Pubblicazione manuale e pianificazione
- Usa **Pubblica** per inviare un post subito.
- Usa **Esegui Pianificazione** per generare la pianificazione del giorno su richiesta.
- L'avvio automatico genera/aggiorna la pianificazione ai due orari configurati.
- Con **Salva Impostazioni** le nuove pianificazioni partono dal giorno successivo; con **Salva + Avvia Oggi** partono subito.
- Le azioni manuali vengono registrate con `trigger = manual`.

## Variabili ambiente (.env)
`TELEGRAM_BOT_TOKEN=...`
`TELEGRAM_CHANNEL_ID=@channel`  # fallback opzionale per Telegram
`TELEGRAM_WEBHOOK_SECRET=...`
`PUBLIC_BASE_URL=https://your-public-url`
`CRON_TIME=0 9 * * *`
`SECOND_SCHEDULE_TIME=18:00`
`MIN_INTERVAL_MINUTES=5`
`CRON_TZ=Europe/Moscow`
`ANALYTICS_SALT=change_me`
`PORT=3000`
`CORS_ORIGIN=*`
`ENABLE_SECURITY_HEADERS=1`
