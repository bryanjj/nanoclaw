import TelegramBot from 'node-telegram-bot-api';
import pino from 'pino';
import { storeGenericMessage, storeChatMetadata } from './db.js';
import { RegisteredGroup } from './types.js';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: { target: 'pino-pretty', options: { colorize: true } }
});

let bot: TelegramBot | null = null;

export interface TelegramCallbacks {
  onMessage: (chatId: string, message: {
    id: string;
    sender: string;
    senderName: string;
    content: string;
    timestamp: string;
  }) => void;
  getRegisteredGroups: () => Record<string, RegisteredGroup>;
}

/**
 * Convert Telegram chat ID to a JID-like identifier for consistency.
 * Format: {chatId}@telegram
 */
export function telegramChatIdToJid(chatId: number): string {
  return `${chatId}@telegram`;
}

/**
 * Extract Telegram chat ID from JID.
 */
export function jidToTelegramChatId(jid: string): number | null {
  if (!jid.endsWith('@telegram')) return null;
  const id = parseInt(jid.replace('@telegram', ''), 10);
  return isNaN(id) ? null : id;
}

/**
 * Send a message to a Telegram chat.
 */
export async function sendTelegramMessage(jid: string, text: string): Promise<void> {
  if (!bot) {
    logger.error('Telegram bot not initialized');
    return;
  }

  const chatId = jidToTelegramChatId(jid);
  if (chatId === null) {
    logger.error({ jid }, 'Invalid Telegram JID');
    return;
  }

  try {
    await bot.sendMessage(chatId, text);
    logger.info({ chatId, length: text.length }, 'Telegram message sent');
  } catch (err) {
    logger.error({ chatId, err }, 'Failed to send Telegram message');
  }
}

/**
 * Set typing indicator for a Telegram chat.
 */
export async function setTelegramTyping(jid: string, isTyping: boolean): Promise<void> {
  if (!bot || !isTyping) return;

  const chatId = jidToTelegramChatId(jid);
  if (chatId === null) return;

  try {
    await bot.sendChatAction(chatId, 'typing');
  } catch (err) {
    logger.debug({ chatId, err }, 'Failed to send typing action');
  }
}

/**
 * Initialize and connect the Telegram bot.
 */
export function connectTelegram(callbacks: TelegramCallbacks): TelegramBot | null {
  const token = process.env.TELEGRAM_BOT_TOKEN;

  if (!token) {
    logger.info('No TELEGRAM_BOT_TOKEN found, Telegram channel disabled');
    return null;
  }

  bot = new TelegramBot(token, { polling: true });

  bot.on('message', (msg) => {
    if (!msg.text) return;

    const chatId = msg.chat.id;
    const jid = telegramChatIdToJid(chatId);
    const timestamp = new Date(msg.date * 1000).toISOString();
    const sender = msg.from?.id?.toString() || '';
    const senderName = msg.from?.first_name || msg.from?.username || sender;
    const isFromMe = false; // Bot messages are handled separately

    // Get chat name
    const chatName = msg.chat.title || msg.chat.username || msg.chat.first_name || jid;

    // Always store chat metadata for discovery
    storeChatMetadata(jid, timestamp, chatName);

    // Only store full message content for registered groups
    const registeredGroups = callbacks.getRegisteredGroups();
    if (registeredGroups[jid]) {
      storeGenericMessage(
        msg.message_id.toString(),
        jid,
        sender,
        senderName,
        msg.text,
        timestamp,
        isFromMe,
        'telegram'
      );

      // Notify the main app about the message
      callbacks.onMessage(jid, {
        id: msg.message_id.toString(),
        sender,
        senderName,
        content: msg.text,
        timestamp
      });
    }
  });

  bot.on('polling_error', (error) => {
    logger.error({ error: error.message }, 'Telegram polling error');
  });

  logger.info('Connected to Telegram');
  return bot;
}

/**
 * Check if Telegram is connected.
 */
export function isTelegramConnected(): boolean {
  return bot !== null;
}
