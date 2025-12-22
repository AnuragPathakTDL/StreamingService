import pino, { type Logger } from "pino";
import type { UploadCompletedEvent } from "../types/upload";
import { ChannelProvisioner } from "./channel-provisioner";
import { NotificationPublisher } from "./notification-publisher";
import type { AlertingService } from "./alerting-service";

export interface PubSubMessage {
  data: string;
  attributes?: Record<string, string>;
  messageId: string;
  publishTime: string;
  deliveryAttempt?: number;
}

export interface EventContext {
  eventId: string;
  timestamp: string;
}

interface UploadEventWorkerOptions {
  provisioner: ChannelProvisioner;
  notificationPublisher: NotificationPublisher;
  alertingService: AlertingService;
  ackDeadlineSeconds: number;
  manifestTtlSeconds: number;
  maxDeliveryAttempts?: number;
  logger?: Logger;
}

export interface WorkerResult {
  action: "ack" | "nack";
  retryInSeconds?: number;
}

export class UploadEventWorker {
  private readonly provisioner: ChannelProvisioner;
  private readonly notificationPublisher: NotificationPublisher;
  private readonly alerting: AlertingService;
  private readonly ackDeadlineSeconds: number;
  private readonly manifestTtlSeconds: number;
  private readonly maxDeliveryAttempts: number;
  private readonly logger: Logger;

  constructor(options: UploadEventWorkerOptions) {
    this.provisioner = options.provisioner;
    this.notificationPublisher = options.notificationPublisher;
    this.alerting = options.alertingService;
    this.ackDeadlineSeconds = options.ackDeadlineSeconds;
    this.manifestTtlSeconds = options.manifestTtlSeconds;
    this.maxDeliveryAttempts = options.maxDeliveryAttempts ?? 5;
    this.logger = options.logger ?? pino({ name: "upload-worker" });
  }

  async handleMessage(
    message: PubSubMessage,
    context?: EventContext
  ): Promise<WorkerResult> {
    const attempt = message.deliveryAttempt ?? 1;
    let event: UploadCompletedEvent | undefined;
    try {
      event = this.parseEvent(message.data);
      this.logger.info(
        {
          contentId: event.data.contentId,
          messageId: message.messageId,
          attempt,
        },
        "Processing UploadService event"
      );
      const metadata = await this.provisioner.provisionFromUpload(event);
      const expiresAt = new Date(
        Date.now() + this.manifestTtlSeconds * 1000
      ).toISOString();
      await this.notificationPublisher.publishPlaybackReady({
        metadata,
        manifestUrl: metadata.playbackUrl,
        expiresAt,
      });
      return { action: "ack" };
    } catch (error) {
      const shouldAck = attempt >= this.maxDeliveryAttempts;
      this.logger.error(
        { err: error, attempt, messageId: message.messageId },
        shouldAck ? "Dropping poison message" : "Upload event failed"
      );
      await this.alerting.ingestFailure(
        event?.data.contentId ?? "unknown",
        error
      );
      if (shouldAck) {
        return { action: "ack" };
      }
      return { action: "nack", retryInSeconds: this.ackDeadlineSeconds };
    }
  }

  private parseEvent(data: string): UploadCompletedEvent {
    const decoded = Buffer.from(data, "base64").toString("utf8");
    const parsed = JSON.parse(decoded) as UploadCompletedEvent;
    if (parsed.eventType !== "media.uploaded") {
      throw new Error(`Unsupported event type ${parsed.eventType}`);
    }
    return parsed;
  }
}
