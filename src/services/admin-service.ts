import { randomUUID } from "node:crypto";
import type { ChannelProvisioner } from "./channel-provisioner";
import type { ChannelMetadataRepository } from "../repositories/channel-metadata-repository";
import type { CdnControlClient } from "../clients/cdn-client";
import type { OvenMediaEngineClient } from "../clients/ome-client";
import type { NotificationPublisher } from "./notification-publisher";
import type { AlertingService } from "./alerting-service";
import type { UploadCompletedEvent } from "../types/upload";
import type { ChannelMetadata } from "../types/channel";

export interface RegisterStreamPayload {
  contentId: string;
  tenantId: string;
  contentType: UploadCompletedEvent["data"]["contentType"];
  sourceGcsUri: string;
  checksum: string;
  durationSeconds: number;
  ingestRegion: string;
  drm?: UploadCompletedEvent["data"]["drm"];
  availabilityWindow?: UploadCompletedEvent["data"]["availabilityWindow"];
  geoRestrictions?: UploadCompletedEvent["data"]["geoRestrictions"];
}

export class StreamAdminService {
  constructor(
    private readonly provisioner: ChannelProvisioner,
    private readonly repository: ChannelMetadataRepository,
    private readonly cdnClient: CdnControlClient,
    private readonly omeClient: OvenMediaEngineClient,
    private readonly notifications: NotificationPublisher,
    private readonly alerting: AlertingService
  ) {}

  async register(payload: RegisterStreamPayload) {
    const event: UploadCompletedEvent = {
      eventId: randomUUID(),
      eventType: "media.uploaded",
      version: "2025-01-01",
      occurredAt: new Date().toISOString(),
      data: {
        contentId: payload.contentId,
        tenantId: payload.tenantId,
        contentType: payload.contentType,
        sourceGcsUri: payload.sourceGcsUri,
        checksum: payload.checksum,
        durationSeconds: payload.durationSeconds,
        ingestRegion: payload.ingestRegion,
        drm: payload.drm,
        availabilityWindow: payload.availabilityWindow,
        geoRestrictions: payload.geoRestrictions,
      },
      acknowledgement: {
        deadlineSeconds: 60,
        required: true,
      },
    };

    const metadata = await this.provisioner.provisionFromUpload(event);
    await this.notifications.publishPlaybackReady({
      metadata,
      manifestUrl: metadata.playbackUrl,
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    return metadata;
  }

  async get(contentId: string): Promise<ChannelMetadata | null> {
    return this.repository.findByContentId(contentId);
  }

  async retire(contentId: string) {
    const record = await this.requireChannel(contentId);
    await this.omeClient.deleteChannel(record.channelId);
    await this.repository.upsert({
      ...record,
      status: "retired",
      lastProvisionedAt: new Date().toISOString(),
    });
    await this.cdnClient.purge(record.manifestPath);
  }

  async purge(contentId: string) {
    const record = await this.requireChannel(contentId);
    await this.cdnClient.purge(record.manifestPath);
  }

  async rotateIngest(contentId: string) {
    const record = await this.requireChannel(contentId);
    await this.omeClient.rotateIngestKey(record.channelId);
    this.alerting.emit({
      event: "stream.ingest.rotated",
      severity: "info",
      description: `Rotated ingest keys for ${contentId}`,
      metadata: { contentId },
    });
  }

  private async requireChannel(contentId: string) {
    const record = await this.repository.findByContentId(contentId);
    if (!record) {
      throw new Error("Stream not found");
    }
    return record;
  }
}
