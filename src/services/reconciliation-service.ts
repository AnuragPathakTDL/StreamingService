import type { ChannelMetadataRepository } from "../repositories/channel-metadata-repository";
import type { ChannelProvisioner } from "./channel-provisioner";
import type { AlertingService } from "./alerting-service";
import type { Logger } from "pino";
import type { UploadCompletedEvent } from "../types/upload";

export class ReconciliationService {
  constructor(
    private readonly repository: ChannelMetadataRepository,
    private readonly provisioner: ChannelProvisioner,
    private readonly alerting: AlertingService,
    private readonly logger: Logger
  ) {}

  async reconcileFailed(limit = 20) {
    const failed = await this.repository.listFailed(limit);
    for (const record of failed) {
      try {
        this.logger.info(
          { contentId: record.contentId },
          "Replaying failed provisioning"
        );
        const replayEvent: UploadCompletedEvent = {
          eventId: `reconcile-${record.contentId}`,
          eventType: "media.uploaded",
          version: "2025-01-01",
          occurredAt: new Date().toISOString(),
          data: {
            contentId: record.contentId,
            tenantId: "pocketlol",
            contentType: record.classification,
            sourceGcsUri: record.sourceAssetUri,
            checksum: record.checksum,
            durationSeconds: 1,
            ingestRegion: record.ingestRegion ?? "us-central1",
            drm: record.drm,
            availabilityWindow: record.availabilityWindow,
            geoRestrictions: record.geoRestrictions,
          },
          acknowledgement: { deadlineSeconds: 60, required: false },
        };
        await this.provisioner.provisionFromUpload(replayEvent);
      } catch (error) {
        this.alerting.ingestFailure(record.contentId, error);
      }
    }
  }
}
