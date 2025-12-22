import { createHash } from "node:crypto";
import pino, { type Logger } from "pino";
import type {
  AbrVariant,
  ChannelMetadata,
  ChannelProvisioningRequest,
  ChannelProvisioningResult,
  ChannelClassification,
} from "../types/channel";
import type { UploadCompletedEvent } from "../types/upload";
import type { ChannelMetadataRepository } from "../repositories/channel-metadata-repository";
import { withExponentialBackoff } from "../utils/retry";
import { OvenMediaEngineClient } from "../clients/ome-client";

interface ChannelProvisionerOptions {
  omeClient: OvenMediaEngineClient;
  repository: ChannelMetadataRepository;
  manifestBucket: string;
  reelsPreset: string;
  seriesPreset: string;
  reelsIngestPool: string;
  seriesIngestPool: string;
  reelsEgressPool: string;
  seriesEgressPool: string;
  maxProvisionRetries: number;
  cdnBaseUrl: string;
  signingKeyId: string;
  dryRun?: boolean;
  logger?: Logger;
}

export class ChannelProvisioner {
  private readonly omeClient: OvenMediaEngineClient;
  private readonly repository: ChannelMetadataRepository;
  private readonly manifestBucket: string;
  private readonly reelPreset: AbrVariant[];
  private readonly seriesPreset: AbrVariant[];
  private readonly reelsIngestPool: string;
  private readonly seriesIngestPool: string;
  private readonly reelsEgressPool: string;
  private readonly seriesEgressPool: string;
  private readonly maxProvisionRetries: number;
  private readonly cdnBaseUrl: string;
  private readonly signingKeyId: string;
  private readonly dryRun: boolean;
  private readonly logger: Logger;

  constructor(options: ChannelProvisionerOptions) {
    this.omeClient = options.omeClient;
    this.repository = options.repository;
    this.manifestBucket = options.manifestBucket;
    this.reelPreset = this.parsePreset(options.reelsPreset);
    this.seriesPreset = this.parsePreset(options.seriesPreset);
    this.reelsIngestPool = options.reelsIngestPool;
    this.seriesIngestPool = options.seriesIngestPool;
    this.reelsEgressPool = options.reelsEgressPool;
    this.seriesEgressPool = options.seriesEgressPool;
    this.maxProvisionRetries = options.maxProvisionRetries;
    this.cdnBaseUrl = options.cdnBaseUrl;
    this.signingKeyId = options.signingKeyId;
    this.dryRun = options.dryRun ?? false;
    this.logger = options.logger ?? pino({ name: "channel-provisioner" });
  }

  async provisionFromUpload(
    event: UploadCompletedEvent
  ): Promise<ChannelMetadata> {
    const classification = event.data.contentType;
    const existing = await this.repository.findByContentId(
      event.data.contentId
    );
    if (
      existing &&
      existing.checksum === event.data.checksum &&
      existing.status === "ready"
    ) {
      this.logger.info(
        { contentId: event.data.contentId },
        "Channel already provisioned with matching checksum"
      );
      return existing;
    }

    const manifestPath = this.buildManifestPath(event.data.contentId);
    const cacheKey = this.buildCacheKey(
      event.data.contentId,
      event.data.checksum
    );
    const abrLadder = this.selectPreset(classification);

    const request: ChannelProvisioningRequest = {
      contentId: event.data.contentId,
      classification,
      sourceUri: event.data.sourceGcsUri,
      ingestPool: this.selectIngestPool(classification),
      egressPool: this.selectEgressPool(classification),
      abrLadder,
      outputBucket: this.manifestBucket,
      manifestPath,
      cacheKey,
      drm: event.data.drm,
      metadata: {
        tenantId: event.data.tenantId,
        checksum: event.data.checksum,
        ingestRegion: event.data.ingestRegion,
        durationSeconds: event.data.durationSeconds.toString(),
        signingKeyId: this.signingKeyId,
        dryRun: this.dryRun ? "true" : "false",
      },
      availabilityWindow: event.data.availabilityWindow,
      geoRestrictions: event.data.geoRestrictions,
    };

    const baseRecord: ChannelMetadata = {
      contentId: event.data.contentId,
      channelId: existing?.channelId ?? "pending",
      classification,
      manifestPath,
      playbackUrl: this.buildPlaybackUrl(manifestPath),
      originEndpoint: existing?.originEndpoint ?? "pending",
      cacheKey,
      checksum: event.data.checksum,
      status: "provisioning",
      retries: existing ? existing.retries + 1 : 0,
      sourceAssetUri: event.data.sourceGcsUri,
      lastProvisionedAt: new Date().toISOString(),
      drm: event.data.drm,
      ingestRegion: event.data.ingestRegion,
      availabilityWindow: event.data.availabilityWindow,
      geoRestrictions: event.data.geoRestrictions,
    };

    await this.repository.upsert(baseRecord);

    let response: ChannelProvisioningResult | undefined;
    try {
      response = await withExponentialBackoff(
        () => this.omeClient.createChannel(request),
        {
          retries: this.maxProvisionRetries,
          onRetry: (err, attempt) =>
            this.logger.warn(
              { err, attempt, contentId: event.data.contentId },
              "Provisioning retry"
            ),
        }
      );
    } catch (error) {
      await this.repository.upsert({
        ...baseRecord,
        status: "failed",
        retries: baseRecord.retries + 1,
        lastProvisionedAt: new Date().toISOString(),
      });
      throw error;
    }

    const finalRecord: ChannelMetadata = {
      ...baseRecord,
      channelId: response.channelId,
      manifestPath: response.manifestPath ?? baseRecord.manifestPath,
      playbackUrl: response.playbackBaseUrl ?? baseRecord.playbackUrl,
      originEndpoint: response.originEndpoint,
      status: "ready",
      lastProvisionedAt: new Date().toISOString(),
    };
    await this.repository.upsert(finalRecord);
    this.logger.info(
      { contentId: finalRecord.contentId, channelId: finalRecord.channelId },
      "Channel provisioned"
    );
    return finalRecord;
  }

  private parsePreset(preset: string): AbrVariant[] {
    return preset
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean)
      .map((entry) => {
        const [name, resolution, bitrate] = entry
          .split("|")
          .map((value) => value.trim());
        if (!name || !resolution || !bitrate) {
          throw new Error(`Invalid ABR preset entry: ${entry}`);
        }
        const bitrateKbps = Number.parseInt(bitrate, 10);
        if (Number.isNaN(bitrateKbps)) {
          throw new Error(`Invalid bitrate in ABR preset entry: ${entry}`);
        }
        return {
          name,
          resolution,
          bitrateKbps,
        } satisfies AbrVariant;
      });
  }

  private selectPreset(classification: ChannelClassification) {
    return classification === "reel" ? this.reelPreset : this.seriesPreset;
  }

  private selectIngestPool(classification: ChannelClassification) {
    return classification === "reel"
      ? this.reelsIngestPool
      : this.seriesIngestPool;
  }

  private selectEgressPool(classification: ChannelClassification) {
    return classification === "reel"
      ? this.reelsEgressPool
      : this.seriesEgressPool;
  }

  private buildManifestPath(contentId: string) {
    return `manifests/${contentId}/master.m3u8`;
  }

  private buildPlaybackUrl(manifestPath: string) {
    return new URL(manifestPath, this.cdnBaseUrl).toString();
  }

  private buildCacheKey(contentId: string, checksum: string) {
    return createHash("sha1").update(`${contentId}:${checksum}`).digest("hex");
  }
}
