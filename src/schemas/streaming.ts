import { z } from "zod";

export const playbackParamsSchema = z.object({
  id: z.string().uuid(),
});

export const playbackQuerySchema = z.object({
  quality: z.enum(["auto", "1080p", "720p", "480p", "360p"]).default("auto").optional(),
  device: z.enum(["mobile", "tablet", "web", "tv"]).optional(),
});

export const playbackResponseSchema = z.object({
  playbackUrl: z.string().url(),
  expiresAt: z.string().datetime(),
  cdn: z.string(),
});

export type PlaybackParams = z.infer<typeof playbackParamsSchema>;
export type PlaybackQuery = z.infer<typeof playbackQuerySchema>;
export type PlaybackResponse = z.infer<typeof playbackResponseSchema>;
