
import { DurableObject } from "cloudflare:workers";

const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json";
const STATUS_KEY = "player_watcher_status";
const SESSION_KEY = "player_watcher_session";

const json = (data, status = 200) =>
  new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(updateWatcher(env));
  },

  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return getWatcher(env, "/health");
    }

    if (url.pathname === "/wake") {
      return getWatcher(env, "/wake");
    }

    return json({
      name: "Riverwood Player Watcher",
      status: "running",
      endpoints: ["/health", "/wake"],
    });
  },
};

async function getWatcher(env, path) {
  if (!env.PRESENCE) {
    return json({ error: "PRESENCE Durable Object binding is missing." }, 500);
  }

  const id = env.PRESENCE.idFromName("player-watcher");
  return env.PRESENCE.get(id).fetch(`https://presence${path}`);
}

async function updateWatcher(env) {
  return getWatcher(env, "/update");
}

export class Presence extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.socket = null;
    this.connecting = false;
    this.heartbeatTimer = null;
    this.heartbeatInterval = null;
    this.heartbeatAcked = true;
    this.sequence = null;
    this.sessionId = null;
    this.resumeGatewayUrl = null;
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return this.health();
    }

    if (url.pathname === "/wake") {
      await this.connectGateway();
      return this.health();
    }

    if (url.pathname === "/update") {
      await this.refreshPresence();
      return this.health();
    }

    return json({ error: "Not found" }, 404);
  }

  async health() {
    const state = await this.ctx.storage.get(STATUS_KEY) || {};

    return json({
      name: "Riverwood Player Watcher",
      status: state.status || "starting",
      discordReady: state.status === "online",
      playerCount: state.playerCount ?? null,
      maxPlayers: state.maxPlayers ?? null,
      lastUpdated: state.lastUpdated || null,
      lastError: state.lastError || null,
    });
  }

  async refreshPresence() {
    const token = this.env.PLAYER_WATCHER_DISCORD_BOT_TOKEN;
    const placeId = this.env.ROBLOX_PLACE_ID;

    if (!token || !placeId) {
      await this.saveStatus({
        status: "error",
        lastError: !token
          ? "Player Watcher Discord token is missing."
          : "ROBLOX_PLACE_ID is missing.",
      });
      return;
    }

    try {
      const universeResponse = await fetch(
        `https://apis.roblox.com/universes/v1/places/${encodeURIComponent(placeId)}/universe`
      );

      if (!universeResponse.ok) {
        throw new Error(`Roblox place lookup failed: ${universeResponse.status}`);
      }

      const universe = await universeResponse.json();

      if (!universe.universeId) {
        throw new Error("Roblox did not return a universe ID for this place.");
      }

      const gameResponse = await fetch(
        `https://games.roblox.com/v1/games?universeIds=${universe.universeId}`
      );

      if (!gameResponse.ok) {
        throw new Error(`Roblox player lookup failed: ${gameResponse.status}`);
      }

      const gameData = await gameResponse.json();
      const game = gameData.data?.[0];

      if (!game) {
        throw new Error("Roblox returned no game information.");
      }

      const playing = Number(game.playing) || 0;
      const maxPlayers = Number(game.maxPlayers) || 0;

      await this.saveStatus({
        playerCount: playing,
        maxPlayers,
        lastUpdated: new Date().toISOString(),
        lastError: null,
      });

      if (
        this.socket &&
        this.socket.readyState === WebSocket.OPEN
      ) {
        this.sendPresence(playing);
      } else {
        await this.connectGateway();
      }
    } catch (error) {
      console.error("[Player Watcher] Refresh failed:", error);

      await this.saveStatus({
        lastError: String(error),
      });

      if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
        await this.connectGateway();
      }
    }
  }

  async saveStatus(changes) {
    const previous = await this.ctx.storage.get(STATUS_KEY) || {};

    await this.ctx.storage.put(STATUS_KEY, {
      ...previous,
      ...changes,
    });
  }

  async connectGateway() {
    if (this.connecting) return;

    if (
      this.socket &&
      (this.socket.readyState === WebSocket.OPEN ||
        this.socket.readyState === WebSocket.CONNECTING)
    ) {
      return;
    }

    const token = this.env.PLAYER_WATCHER_DISCORD_BOT_TOKEN;

    if (!token) {
      await this.saveStatus({
        status: "error",
        lastError: "Discord bot token is missing.",
      });
      return;
    }

    this.connecting = true;

    try {
      const session = await this.ctx.storage.get(SESSION_KEY);

      this.sessionId = session?.sessionId || null;
      this.resumeGatewayUrl = session?.resumeGatewayUrl || null;
      this.sequence = session?.sequence ?? null;

      await this.saveStatus({
        status: "connecting",
        lastError: null,
      });

      const gateway = this.sessionId && this.resumeGatewayUrl
        ? this.resumeGatewayUrl
        : GATEWAY_URL;

      const ws = new WebSocket(gateway);
      this.socket = ws;

      ws.addEventListener("message", (event) => {
        this.handleMessage(event.data).catch((error) => {
          console.error("[Player Watcher] Message error:", error);
          this.saveStatus({ lastError: String(error) });
        });
      });

      ws.addEventListener("close", (event) => {
        this.handleClose(event).catch(console.error);
      });

      ws.addEventListener("error", () => {
        this.saveStatus({
          lastError: "Discord Gateway WebSocket error.",
        });
      });
    } catch (error) {
      await this.saveStatus({
        status: "error",
        lastError: String(error),
      });
      await this.ctx.storage.setAlarm(Date.now() + 10000);
    } finally {
      this.connecting = false;
    }
  }

  async handleMessage(raw) {
    const packet = JSON.parse(
      typeof raw === "string" ? raw : String(raw)
    );

    const { op, t, s, d } = packet;

    if (s !== null && s !== undefined) {
      this.sequence = s;
    }

    if (op === 0) {
      if (t === "READY") {
        this.sessionId = d.session_id;
        this.resumeGatewayUrl = d.resume_gateway_url;

        await this.ctx.storage.put(SESSION_KEY, {
          sessionId: this.sessionId,
          resumeGatewayUrl: this.resumeGatewayUrl,
          sequence: this.sequence,
        });

        await this.saveStatus({
          status: "online",
          botUser: d.user?.username || null,
          lastError: null,
        });

        const state = await this.ctx.storage.get(STATUS_KEY) || {};
        this.sendPresence(state.playerCount ?? 0);
      }

      if (t === "RESUMED") {
        await this.saveStatus({
          status: "online",
          lastError: null,
        });

        const state = await this.ctx.storage.get(STATUS_KEY) || {};
        this.sendPresence(state.playerCount ?? 0);
      }

      await this.ctx.storage.put(SESSION_KEY, {
        sessionId: this.sessionId,
        resumeGatewayUrl: this.resumeGatewayUrl,
        sequence: this.sequence,
      });

      return;
    }

    if (op === 1) {
      this.send({ op: 1, d: this.sequence });
      return;
    }

    if (op === 7 || op === 9) {
      if (op === 9 && !d) {
        this.sessionId = null;
        this.resumeGatewayUrl = null;
        this.sequence = null;
        await this.ctx.storage.delete(SESSION_KEY);
      }

      this.socket?.close(4000, "Discord requested reconnect");
      return;
    }

    if (op === 10) {
      this.heartbeatInterval = d.heartbeat_interval;
      this.heartbeatAcked = true;
      this.startHeartbeat();

      const token = this.env.PLAYER_WATCHER_DISCORD_BOT_TOKEN;
      const authToken = token.startsWith("Bot ") ? token : `Bot ${token}`;

      if (this.sessionId && this.sequence !== null) {
        this.send({
          op: 6,
          d: {
            token: authToken,
            session_id: this.sessionId,
            seq: this.sequence,
          },
        });
      } else {
        this.send({
          op: 2,
          d: {
            token: authToken,
            intents: 0,
            properties: {
              os: "linux",
              browser: "riverwood-player-watcher",
              device: "riverwood-player-watcher",
            },
          },
        });
      }

      return;
    }

    if (op === 11) {
      this.heartbeatAcked = true;
    }
  }

  sendPresence(playerCount) {
    this.send({
      op: 3,
      d: {
        since: null,
        activities: [
          {
            name: `${playerCount} players in Riverwood County RP`,
            type: 0,
          },
        ],
        status: "online",
        afk: false,
      },
    });
  }

  startHeartbeat() {
    this.stopHeartbeat();

    if (!this.heartbeatInterval) return;

    const delay = Math.floor(Math.random() * this.heartbeatInterval);

    this.heartbeatTimer = setTimeout(() => {
      this.heartbeatLoop();
    }, delay);
  }

  async heartbeatLoop() {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;

    if (!this.heartbeatAcked) {
      this.socket.close(4000, "Heartbeat acknowledgement missed");
      return;
    }

    this.heartbeatAcked = false;
    this.send({ op: 1, d: this.sequence });

    this.heartbeatTimer = setTimeout(
      () => this.heartbeatLoop(),
      this.heartbeatInterval
    );
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  send(payload) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;

    try {
      this.socket.send(JSON.stringify(payload));
    } catch (error) {
      console.error("[Player Watcher] Send failed:", error);
    }
  }

  async handleClose(event) {
    this.stopHeartbeat();
    this.socket = null;
    this.heartbeatAcked = true;

    const fatal = [4004, 4010, 4011, 4013, 4014].includes(event.code);

    await this.saveStatus({
      status: fatal ? "error" : "reconnecting",
      lastError: event.reason ||
        `Discord Gateway closed with code ${event.code}.`,
    });

    if (event.code === 4007 || event.code === 4009) {
      this.sessionId = null;
      this.resumeGatewayUrl = null;
      this.sequence = null;
      await this.ctx.storage.delete(SESSION_KEY);
    }

    if (!fatal) {
      await this.ctx.storage.setAlarm(Date.now() + 5000);
    }
  }

  async alarm() {
    await this.connectGateway();
    await this.refreshPresence();
  }
}
