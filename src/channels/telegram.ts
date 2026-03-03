import TelegramBot from 'node-telegram-bot-api';
import pino from 'pino';
import path from 'path';
import fs from 'fs';
import { storeGenericMessage, storeChatMetadata } from '../db.js';
import { GROUPS_DIR } from '../config.js';
import { registerChannel, ChannelInstance, ChannelCallbacks } from './registry.js';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true } }
});

function chatIdToJid(chatId: number): string {
  return `${chatId}@telegram`;
}

function jidToChatId(jid: string): number | null {
  if (!jid.endsWith('@telegram')) return null;
  const id = parseInt(jid.replace('@telegram', ''), 10);
  return isNaN(id) ? null : id;
}

class TelegramChannel implements ChannelInstance {
  name = 'telegram';
  private bot: TelegramBot;
  private connected = false;

  constructor(token: string, private callbacks: ChannelCallbacks) {
    this.bot = new TelegramBot(token, {
      polling: {
        params: {
          allowed_updates: ['message', 'message_reaction', 'message_reaction_count']
        }
      }
    });
  }

  connect(): void {
    this.bot.on('message', async (msg) => {
      const chatId = msg.chat.id;
      const jid = chatIdToJid(chatId);
      const timestamp = new Date(msg.date * 1000).toISOString();
      const sender = msg.from?.id?.toString() || '';
      const senderName = msg.from?.first_name || msg.from?.username || sender;
      const isFromMe = false;

      const chatName = msg.chat.title || msg.chat.username || msg.chat.first_name || jid;
      storeChatMetadata(jid, timestamp, chatName);

      let content = msg.text || '';

      // Handle photos
      if (msg.photo && msg.photo.length > 0) {
        try {
          const photo = msg.photo[msg.photo.length - 1];
          const fileId = photo.file_id;

          const imagesDir = path.join(GROUPS_DIR, 'main', 'images');
          try {
            if (!fs.existsSync(imagesDir)) {
              fs.mkdirSync(imagesDir, { recursive: true });
            }
          } catch (mkdirErr) {
            logger.warn({ imagesDir, mkdirErr }, 'Could not create images directory, it may already exist');
          }

          const file = await this.bot.getFile(fileId);

          if (file.file_path) {
            const fileUrl = `https://api.telegram.org/file/bot${process.env.TELEGRAM_BOT_TOKEN}/${file.file_path}`;
            const localPath = path.join(imagesDir, `${fileId}.jpg`);

            const response = await fetch(fileUrl);
            const buffer = await response.arrayBuffer();
            fs.writeFileSync(localPath, Buffer.from(buffer));

            const containerPath = `/workspace/group/images/${fileId}.jpg`;
            content = `[Image: ${containerPath}]${msg.caption ? '\n' + msg.caption : ''}`;
            logger.info({ chatId, hostPath: localPath, containerPath }, 'Photo downloaded');
          } else {
            content = `[Image - no file path]${msg.caption ? '\n' + msg.caption : ''}`;
          }
        } catch (err) {
          const errorMsg = err instanceof Error ? err.message : String(err);
          const errorStack = err instanceof Error ? err.stack : undefined;
          logger.error({ chatId, err, errorMsg, errorStack }, 'Failed to download photo');
          content = `[Image - download failed: ${errorMsg}]${msg.caption ? '\n' + msg.caption : ''}`;
        }
      }

      if (!content) return;

      const registeredGroups = this.callbacks.getRegisteredGroups();
      if (registeredGroups[jid]) {
        storeGenericMessage(
          msg.message_id.toString(),
          jid,
          sender,
          senderName,
          content,
          timestamp,
          isFromMe,
          'telegram'
        );

        this.callbacks.onMessage(jid, {
          id: msg.message_id.toString(),
          sender,
          senderName,
          content,
          timestamp
        });
      }
    });

    // Handle emoji reactions
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    this.bot.on('message_reaction', async (reaction: any) => {
      const chatId = reaction.chat?.id;
      const jid = chatId ? chatIdToJid(chatId) : null;
      if (!jid) return;

      const registeredGroups = this.callbacks.getRegisteredGroups();
      if (!registeredGroups[jid]) return;

      const timestamp = new Date(reaction.date * 1000).toISOString();
      const sender = reaction.user?.id?.toString() || reaction.actor_chat?.id?.toString() || '';
      const senderName = reaction.user?.first_name || reaction.user?.username || sender;
      const messageId = reaction.message_id;

      const newReactions: string[] = (reaction.new_reaction || [])
        .map((r: { type: string; emoji?: string }) => r.type === 'emoji' ? r.emoji : null)
        .filter(Boolean);

      if (newReactions.length === 0) return;

      const content = `[Reaction: ${newReactions.join(' ')} on message ${messageId}]`;

      storeGenericMessage(
        `reaction-${messageId}-${sender}-${Date.now()}`,
        jid,
        sender,
        senderName,
        content,
        timestamp,
        false,
        'telegram'
      );

      logger.info({ chatId, messageId, reactions: newReactions, sender }, 'Telegram reaction received');

      this.callbacks.onMessage(jid, {
        id: `reaction-${messageId}-${sender}`,
        sender,
        senderName,
        content,
        timestamp
      });
    });

    this.bot.on('polling_error', (error) => {
      logger.error({ error: error.message }, 'Telegram polling error');
    });

    this.connected = true;
    logger.info('Connected to Telegram');
  }

  async sendMessage(jid: string, text: string): Promise<void> {
    const chatId = jidToChatId(jid);
    if (chatId === null) {
      logger.error({ jid }, 'Invalid Telegram JID');
      return;
    }

    try {
      await this.bot.sendMessage(chatId, text);
      logger.info({ chatId, length: text.length }, 'Telegram message sent');
    } catch (err) {
      logger.error({ chatId, err }, 'Failed to send Telegram message');
    }
  }

  async sendPhoto(jid: string, photoPath: string, caption?: string): Promise<void> {
    const chatId = jidToChatId(jid);
    if (chatId === null) {
      logger.error({ jid }, 'Invalid Telegram JID');
      return;
    }

    try {
      await this.bot.sendPhoto(chatId, photoPath, caption ? { caption } : undefined);
      logger.info({ chatId, photoPath }, 'Telegram photo sent');
    } catch (err) {
      logger.error({ chatId, photoPath, err }, 'Failed to send Telegram photo');
    }
  }

  async sendReaction(jid: string, messageId: string, emoji: string): Promise<void> {
    const chatId = jidToChatId(jid);
    if (chatId === null) {
      logger.error({ jid }, 'Invalid Telegram JID');
      return;
    }

    try {
      await this.bot.setMessageReaction(chatId, parseInt(messageId, 10), {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        reaction: [{ type: 'emoji', emoji: emoji as any }]
      });
      logger.info({ chatId, messageId, emoji }, 'Telegram reaction sent');
    } catch (err) {
      logger.error({ chatId, messageId, emoji, err }, 'Failed to send Telegram reaction');
    }
  }

  async setTyping(jid: string, isTyping: boolean): Promise<void> {
    if (!isTyping) return;

    const chatId = jidToChatId(jid);
    if (chatId === null) return;

    try {
      await this.bot.sendChatAction(chatId, 'typing');
    } catch (err) {
      logger.debug({ chatId, err }, 'Failed to send typing action');
    }
  }

  isConnected(): boolean {
    return this.connected;
  }

  ownsJid(jid: string): boolean {
    return jid.endsWith('@telegram');
  }
}

// Self-register when this module is imported
registerChannel('telegram', (callbacks) => {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  if (!token) {
    logger.info('No TELEGRAM_BOT_TOKEN found, Telegram channel disabled');
    return null;
  }
  const channel = new TelegramChannel(token, callbacks);
  channel.connect();
  return channel;
});
