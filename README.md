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
- Ora di avvio automatico: quando viene generata la pianificazione giornaliera.
- Intervallo minimo: minuti minimi tra due post (mai due post allo stesso minuto).
- Le aziende possono avere un orario preferito; in caso di conflitto l'orario viene spostato in avanti rispettando l'intervallo.
- Rotazione matematica dei post:
  - i post **con orario preferito** restano fissi nei propri slot;
  - i post **senza orario preferito** ruotano ciclicamente giorno per giorno tra gli slot disponibili, così da coprire fasce orarie diverse e aumentare la reach.

## Analitiche link
- Attiva il tracciamento link nel form del post.
- I link CTA vengono riscritti come `/r/:code` e i click vengono salvati.
- `PUBLIC_BASE_URL` deve essere configurato per rendere raggiungibili i link tracciati.
- `ANALYTICS_SALT` viene usato per hashare gli IP (privacy).

## Pubblicazione manuale e pianificazione
- Usa **Pubblica** per inviare un post subito.
- Usa **Esegui Pianificazione** per generare la pianificazione del giorno su richiesta.
- L'avvio automatico genera la pianificazione all'ora configurata.
- Le azioni manuali vengono registrate con `trigger = manual`.

## Variabili ambiente (.env)
`TELEGRAM_BOT_TOKEN=...`
`TELEGRAM_CHANNEL_ID=@channel`  # fallback opzionale per Telegram
`TELEGRAM_WEBHOOK_SECRET=...`
`PUBLIC_BASE_URL=https://your-public-url`
`CRON_TIME=0 9 * * *`
`CRON_TZ=Europe/Moscow`
`ANALYTICS_SALT=change_me`
`PORT=3000`
`CORS_ORIGIN=*`
`ENABLE_SECURITY_HEADERS=1`
