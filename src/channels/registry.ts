import { RegisteredGroup } from '../types.js';

export interface InboundMessage {
  id: string;
  sender: string;
  senderName: string;
  content: string;
  timestamp: string;
}

export interface ChannelCallbacks {
  onMessage: (chatJid: string, message: InboundMessage) => void;
  getRegisteredGroups: () => Record<string, RegisteredGroup>;
}

export interface ChannelInstance {
  name: string;
  connect(): void;
  sendMessage(jid: string, text: string): Promise<void>;
  sendPhoto(jid: string, photoPath: string, caption?: string): Promise<void>;
  sendReaction?(jid: string, messageId: string, emoji: string): Promise<void>;
  setTyping?(jid: string, isTyping: boolean): Promise<void>;
  isConnected(): boolean;
  ownsJid(jid: string): boolean;
}

export type ChannelFactory = (callbacks: ChannelCallbacks) => ChannelInstance | null;

const registry = new Map<string, ChannelFactory>();

export function registerChannel(name: string, factory: ChannelFactory): void {
  registry.set(name, factory);
}

export function getRegisteredChannelNames(): string[] {
  return [...registry.keys()];
}

export function getChannelFactory(name: string): ChannelFactory | undefined {
  return registry.get(name);
}
