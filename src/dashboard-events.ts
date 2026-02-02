/**
 * Dashboard Events
 * Central event emitter for real-time dashboard updates
 */

import { EventEmitter } from 'events';

export interface DashboardEvent {
  type: 'container_spawn' | 'container_complete' | 'agent_event' | 'message_received' | 'message_sent';
  timestamp: string;
  groupFolder?: string;
  chatJid?: string;
  data: unknown;
}

class DashboardEventEmitter extends EventEmitter {
  private recentEvents: DashboardEvent[] = [];
  private maxEvents = 100;

  emitDashboard(event: DashboardEvent): void {
    this.recentEvents.push(event);
    if (this.recentEvents.length > this.maxEvents) {
      this.recentEvents.shift();
    }
    this.emit('dashboard', event);
  }

  getRecentEvents(): DashboardEvent[] {
    return [...this.recentEvents];
  }
}

export const dashboardEvents = new DashboardEventEmitter();
