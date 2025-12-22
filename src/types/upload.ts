import type { ChannelClassification } from "./channel";

export interface UploadCompletedEvent {
  eventId: string;
  eventType: "media.uploaded";
  version: string;
  occurredAt: string;
  data: {
    contentId: string;
    tenantId: string;
    contentType: ChannelClassification;
    sourceGcsUri: string;
    checksum: string;
    durationSeconds: number;
    ingestRegion: string;
    drm?: {
      keyId: string;
      licenseServer: string;
    };
    tags?: string[];
    variants?: Array<{
      codec: string;
      bitrateKbps: number;
      height: number;
      width: number;
    }>;
    availabilityWindow?: {
      startsAt: string;
      endsAt: string;
    };
    geoRestrictions?: {
      allow?: string[];
      deny?: string[];
    };
  };
  acknowledgement?: {
    deadlineSeconds: number;
    required: boolean;
  };
}

export interface UploadEventEnvelope {
  id: string;
  publishTime: string;
  message: UploadCompletedEvent;
}
