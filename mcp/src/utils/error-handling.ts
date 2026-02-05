// Error handling utilities

import { SearchResponse } from "../api/types.js";
import { ERROR_MESSAGES } from "../config/constants.js";

export function formatSearchResponse(searchResponse: SearchResponse, responseType: string) {
    if (responseType === "completion") {
        // For AI completion, return the completion text
        return {
            content: [
                {
                    type: "text" as const,
                    text: searchResponse.completion || "No response generated",
                },
            ],
        };
    } else {
        // For raw results, format the search results simply
        const formattedResults = searchResponse.results
            .map((result: any, index) => {
                const parts = [
                    `**Result ${index + 1}${result.score ? ` (Score: ${result.score.toFixed(3)})` : ""}:**`
                ];

                // Add metadata if available
                if (result.entity_id || result.name) {
                    const metadata = [];
                    if (result.entity_id) metadata.push(`ID: ${result.entity_id}`);
                    if (result.name) metadata.push(`Name: ${result.name}`);
                    parts.push(metadata.join(" | "));
                }

                // Extract content from various possible fields
                const content = result.textual_representation || 
                               result.content || 
                               result.text || 
                               result.payload?.content || 
                               result.payload?.text;
                
                if (content) {
                    parts.push(content);
                } else {
                    // Fallback to stringified JSON, but truncate if too long
                    const jsonStr = JSON.stringify(result, null, 2);
                    parts.push(jsonStr.length > 500 ? jsonStr.substring(0, 500) + "..." : jsonStr);
                }

                return parts.join("\n");
            })
            .join("\n\n---\n\n");

        const summaryText = [
            `**Collection:** ${process.env.AIRWEAVE_COLLECTION}`,
            `**Results:** ${searchResponse.results.length}`,
            "",
            formattedResults || "No results found.",
        ].join("\n");

        return {
            content: [
                {
                    type: "text" as const,
                    text: summaryText,
                },
            ],
        };
    }
}

export function formatErrorResponse(error: Error, searchRequest: any) {
    const errorMessage = error.message;

    return {
        content: [
            {
                type: "text" as const,
                text: `**Error:** Failed to search collection.\n\n**Details:** ${errorMessage}\n\n**Debugging Info:**\n- Collection: ${process.env.AIRWEAVE_COLLECTION}\n- Base URL: ${process.env.AIRWEAVE_BASE_URL || "https://api.airweave.ai"}\n- Endpoint: /collections/${process.env.AIRWEAVE_COLLECTION}/search\n- Parameters: ${JSON.stringify(searchRequest, null, 2)}`,
            },
        ],
    };
}

// Validation errors are now handled directly in the search tool
