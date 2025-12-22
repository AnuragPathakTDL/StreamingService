export type ChannelClassification = "reel" | "series";

export interface AbrVariant {
  /** Human readable label such as 1080p or 720p */
  name: string;
  /** Resolution formatted as WIDTHxHEIGHT */
  resolution: string;
  /** Average target bitrate in kilobits per second */
  bitrateKbps: number;
}

export interface ChannelProvisioningRequest {
  contentId: string;
  classification: ChannelClassification;
  sourceUri: string;
  ingestPool: string;
  egressPool: string;
  abrLadder: AbrVariant[];
  outputBucket: string;
  manifestPath: string;
  cacheKey: string;
  drm?: {
    keyId: string;
    licenseServer: string;
  };
  metadata: Record<string, string>;
  availabilityWindow?: {
    startsAt: string;
    endsAt: string;
  };
  geoRestrictions?: {
    allow?: string[];
    deny?: string[];
  };
}

export interface ChannelProvisioningResult {
  channelId: string;
  manifestPath: string;
  originEndpoint: string;
  playbackBaseUrl: string;
  profileHash: string;
}

export type ProvisioningStatus =
  | "provisioning"
  | "ready"
  | "failed"
  | "retired";

export interface ChannelMetadata {
  contentId: string;
  channelId: string;
  classification: ChannelClassification;
  manifestPath: string;
  playbackUrl: string;
  originEndpoint: string;
  cacheKey: string;
  checksum: string;
  status: ProvisioningStatus;
  retries: number;
  sourceAssetUri: string;
  lastProvisionedAt: string;
  drm?: {
    keyId: string;
    licenseServer: string;
  };
  ingestRegion?: string;
  availabilityWindow?: {
    startsAt: string;
    endsAt: string;
  };
  geoRestrictions?: {
    allow?: string[];
    deny?: string[];
  };
}
