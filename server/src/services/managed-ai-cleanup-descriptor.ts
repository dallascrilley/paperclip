import { z } from "zod";

/** Host-created authority for one private home. Contains no credential value. */
export const managedAiCleanupDescriptorSchema = z.object({
  companyId: z.string().uuid(), grantId: z.string().uuid(), connectionId: z.string().uuid(),
  homeName: z.string().regex(/^paperclip-ai-[a-f0-9-]+-[A-Za-z0-9]+$/),
  ownershipToken: z.string().uuid(), provider: z.string(), subscriptionFile: z.boolean(),
  originalCredentialHash: z.string().regex(/^[a-f0-9]{64}$/),
}).strict();
export type ManagedAiCleanupDescriptor = z.infer<typeof managedAiCleanupDescriptorSchema>;
