import { z } from "zod";
import type { PiiCheckResult, PiiEntity } from "@/types";

// Schema for PII entity from Azure response
const PiiEntitySchema = z.object({
  text: z.string(),
  category: z.string(),
  offset: z.number(),
  length: z.number(),
  confidenceScore: z.number(),
});

// Schema for Azure PII response
const AzurePiiResponseSchema = z.object({
  kind: z.literal("PiiEntityRecognitionResults"),
  results: z.object({
    documents: z.array(
      z.object({
        id: z.string(),
        redactedText: z.string(),
        entities: z.array(PiiEntitySchema),
        warnings: z.array(z.unknown()),
      })
    ),
    errors: z.array(z.unknown()),
    modelVersion: z.string(),
  }),
});

// PII categories to check for (banking-relevant)
export const BANKING_PII_CATEGORIES = [
  "Person",
  "PersonType",
  "PhoneNumber",
  "Email",
  "Address",
  "USBankAccountNumber",
  "CreditCardNumber",
  "USSocialSecurityNumber",
  "USDriversLicenseNumber",
  "USPassportNumber",
  "USIndividualTaxpayerIdentification",
  "InternationalBankingAccountNumber",
  "SWIFTCode",
  "IPAddress",
] as const;

export type BankingPiiCategory = (typeof BANKING_PII_CATEGORIES)[number];

interface CheckPiiOptions {
  text: string;
  categories?: BankingPiiCategory[];
  confidenceThreshold?: number;
}

interface FallbackPattern {
  category: BankingPiiCategory;
  regex: RegExp;
  validate?: (value: string) => boolean;
}

const FALLBACK_PII_PATTERNS: FallbackPattern[] = [
  {
    category: "Email",
    regex: /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi,
  },
  {
    category: "PhoneNumber",
    regex: /\b(?:\+?\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?){2}\d{4}\b/g,
  },
  {
    category: "USSocialSecurityNumber",
    regex: /\b\d{3}-\d{2}-\d{4}\b/g,
  },
  {
    category: "CreditCardNumber",
    regex: /\b(?:\d[ -]*?){13,19}\b/g,
    validate: (value: string) => isLikelyCreditCard(value),
  },
  {
    category: "InternationalBankingAccountNumber",
    regex: /\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b/gi,
  },
  {
    category: "IPAddress",
    regex: /\b(?:\d{1,3}\.){3}\d{1,3}\b/g,
    validate: (value: string) => isValidIpv4(value),
  },
];

function isLikelyCreditCard(value: string): boolean {
  const digits = value.replace(/\D/g, "");
  if (digits.length < 13 || digits.length > 19) {
    return false;
  }

  let sum = 0;
  let shouldDouble = false;
  for (let i = digits.length - 1; i >= 0; i -= 1) {
    let digit = Number(digits[i]);
    if (shouldDouble) {
      digit *= 2;
      if (digit > 9) {
        digit -= 9;
      }
    }
    sum += digit;
    shouldDouble = !shouldDouble;
  }

  return sum % 10 === 0;
}

function isValidIpv4(value: string): boolean {
  return value.split(".").every((part) => {
    const num = Number(part);
    return Number.isInteger(num) && num >= 0 && num <= 255;
  });
}

function applyFallbackRedaction(text: string, entities: PiiEntity[]): string {
  if (entities.length === 0) {
    return text;
  }

  let redacted = text;
  const sorted = [...entities].sort((a, b) => b.offset - a.offset);
  for (const entity of sorted) {
    const before = redacted.slice(0, entity.offset);
    const after = redacted.slice(entity.offset + entity.length);
    redacted = `${before}[REDACTED]${after}`;
  }

  return redacted;
}

function runFallbackPiiDetection(text: string, categories: BankingPiiCategory[]): PiiCheckResult {
  const detected: PiiEntity[] = [];
  const seen = new Set<string>();

  for (const pattern of FALLBACK_PII_PATTERNS) {
    if (!categories.includes(pattern.category)) {
      continue;
    }

    pattern.regex.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.regex.exec(text)) !== null) {
      const value = match[0];
      if (!value) {
        continue;
      }

      if (pattern.validate && !pattern.validate(value)) {
        continue;
      }

      const key = `${pattern.category}:${match.index}:${value.length}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);

      detected.push({
        text: value,
        category: pattern.category,
        offset: match.index,
        length: value.length,
        confidenceScore: 0.99,
      });
    }
  }

  return {
    hasPii: detected.length > 0,
    entities: detected,
    redactedText: applyFallbackRedaction(text, detected),
  };
}

/**
 * Get Azure AD access token for Cognitive Services
 */
async function getAzureAccessToken(): Promise<string | null> {
  // Check for cached token first
  const cachedToken = process.env.AZURE_ACCESS_TOKEN;
  if (cachedToken) {
    return cachedToken;
  }

  // Try to get token using Azure CLI (for local dev)
  try {
    const { execSync } = await import("child_process");
    const token = execSync(
      'az account get-access-token --resource https://cognitiveservices.azure.com --query accessToken -o tsv',
      { encoding: 'utf-8', timeout: 10000 }
    ).trim();
    return token || null;
  } catch {
    return null;
  }
}

/**
 * Check text for PII using Azure Language Service (cloud or container)
 */
export async function checkPii({
  text,
  categories = [...BANKING_PII_CATEGORIES],
  confidenceThreshold = 0.8,
}: CheckPiiOptions): Promise<PiiCheckResult> {
  const containerEndpoint = process.env.PII_CONTAINER_ENDPOINT;
  const endpoint = process.env.PII_ENDPOINT || containerEndpoint || "http://localhost:5000";
  const apiKey = process.env.PII_API_KEY || "";

  // Check if we're using a container (no auth needed - container handles billing internally)
  const isContainer = containerEndpoint && endpoint === containerEndpoint;

  // Build headers - support container (no auth), API key, or Azure AD auth
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  if (!isContainer) {
    // Cloud endpoint - needs authentication
    if (apiKey) {
      headers["Ocp-Apim-Subscription-Key"] = apiKey;
    } else {
      // Try Azure AD authentication
      const token = await getAzureAccessToken();
      if (token) {
        headers["Authorization"] = `Bearer ${token}`;
      } else {
        console.warn("No PII API key or Azure AD token available");
        return runFallbackPiiDetection(text, categories);
      }
    }
  }
  // Container endpoint - no auth needed, container handles billing to Azure resource

  try {
    // Build request body - container uses all categories by default, cloud can filter
    const requestBody: Record<string, unknown> = {
      kind: "PiiEntityRecognition",
      parameters: {
        modelVersion: "latest",
        // Only specify categories for cloud endpoint - container may not support all
        ...(isContainer ? {} : { piiCategories: categories }),
      },
      analysisInput: {
        documents: [
          {
            id: "1",
            language: "en",
            text: text,
          },
        ],
      },
    };

    const response = await fetch(
      `${endpoint}/language/:analyze-text?api-version=2023-04-01`,
      {
        method: "POST",
        headers,
        body: JSON.stringify(requestBody),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error("PII check failed:", response.status, response.statusText, errorText);
      return runFallbackPiiDetection(text, categories);
    }

    const data = await response.json();
    const parsed = AzurePiiResponseSchema.safeParse(data);

    if (!parsed.success) {
      console.error("Failed to parse PII response:", parsed.error);
      return runFallbackPiiDetection(text, categories);
    }

    const document = parsed.data.results.documents[0];
    if (!document) {
      return {
        hasPii: false,
        entities: [],
      };
    }

    // Filter entities by confidence threshold AND banking-relevant categories
    // This prevents false positives like "NVIDIA", "IMF", "Conservative" being flagged
    const filteredEntities: PiiEntity[] = document.entities
      .filter((e) =>
        e.confidenceScore >= confidenceThreshold &&
        categories.includes(e.category as BankingPiiCategory)
      )
      .map((e) => ({
        text: e.text,
        category: e.category,
        offset: e.offset,
        length: e.length,
        confidenceScore: e.confidenceScore,
      }));

    return {
      hasPii: filteredEntities.length > 0,
      entities: filteredEntities,
      redactedText: document.redactedText,
    };
  } catch (error) {
    console.error("PII check error:", error);
    return runFallbackPiiDetection(text, categories);
  }
}

/**
 * Format PII detection result for user-facing message
 */
export function formatPiiWarning(entities: PiiEntity[]): string {
  const categories = [...new Set(entities.map((e) => formatCategory(e.category)))];

  if (categories.length === 0) {
    return "Your message contains sensitive information that cannot be processed.";
  }

  if (categories.length === 1) {
    return `Your message contains ${categories[0]} information which cannot be processed for security reasons.`;
  }

  const lastCategory = categories.pop();
  return `Your message contains ${categories.join(", ")} and ${lastCategory} information which cannot be processed for security reasons.`;
}

/**
 * Format category name for display
 */
function formatCategory(category: string): string {
  const categoryMap: Record<string, string> = {
    Person: "personal name",
    PersonType: "personal",
    PhoneNumber: "phone number",
    Email: "email address",
    Address: "address",
    USBankAccountNumber: "bank account number",
    CreditCardNumber: "credit card",
    USSocialSecurityNumber: "Social Security Number",
    USDriversLicenseNumber: "driver's license",
    USPassportNumber: "passport number",
    USIndividualTaxpayerIdentification: "tax ID",
    InternationalBankingAccountNumber: "IBAN",
    SWIFTCode: "SWIFT code",
    IPAddress: "IP address",
  };

  return categoryMap[category] || category.toLowerCase();
}
