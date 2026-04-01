package com.asar.casedispositioncloser.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Component
public class CaseApiClient {

    private final WebClient client;

    public CaseApiClient(Environment env) {
        String baseUrl = must(env.getProperty("case-api.base-url"));
        String user = must(env.getProperty("case-api.username"));
        String pass = must(env.getProperty("case-api.password"));

        this.client = WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeaders(h -> h.setBasicAuth(user, pass))
                .build();
    }

    /**
     * Original method kept for backwards compatibility.
     * Fetches all cases without filtering — avoid using this directly
     * as it will hit SAP's 10k $skip limit when case volume is high.
     */
    public JsonNode getCases(int top, int skip) {
        return getCasesWithFilter(top, skip, null);
    }

    /**
     * Fetches cases with an optional OData $filter applied at the SAP level.
     * Use this instead of getCases() to avoid fetching all cases and
     * hitting SAP's hard 10k $skip pagination limit.
     *
     * @param top    page size
     * @param skip   offset
     * @param filter OData filter string e.g. "status ne 'Z5'" — pass null for no filter
     */
    public JsonNode getCasesWithFilter(int top, int skip, String filter) {
        StringBuilder uri = new StringBuilder("/sap/c4c/api/v1/case-service/cases")
                .append("?$top=").append(top)
                .append("&$skip=").append(skip)
                .append("&$select=id,displayId,status,statusSchema,lifeCycleStatus,extensions,adminData");

        if (filter != null && !filter.isBlank()) {
            uri.append("&$filter=").append(filter);
        }

        try {
            System.out.println("GET " + uri);

            return client.get()
                    .uri(uri.toString())
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();

        } catch (WebClientResponseException e) {
            System.err.println("GET /cases failed status=" + e.getStatusCode());
            System.err.println("Response body: " + e.getResponseBodyAsString());
            throw e;
        }
    }

    public JsonNode getCaseById(String caseId) {
        return client.get()
                .uri("/sap/c4c/api/v1/case-service/cases/{id}", caseId)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();
    }

    public ResponseEntity<JsonNode> getCaseByIdWithHeaders(String caseId) {
        return client.get()
                .uri("/sap/c4c/api/v1/case-service/cases/{id}", caseId)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toEntity(JsonNode.class)
                .block();
    }

    public JsonNode patchCaseWithEtag(String caseId, JsonNode patchBody) {
        ResponseEntity<JsonNode> getResp = getCaseByIdWithHeaders(caseId);

        String etag = getResp.getHeaders().getETag();
        if (etag == null || etag.isBlank()) {
            throw new RuntimeException("No ETag returned by GET for case " + caseId + ". Cannot PATCH.");
        }

        try {
            return client.patch()
                    .uri("/sap/c4c/api/v1/case-service/cases/{id}", caseId)
                    .header("If-Match", etag)
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .bodyValue(patchBody)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();

        } catch (WebClientResponseException e) {
            System.err.println("PATCH failed. caseId=" + caseId);
            System.err.println("If-Match used: " + etag);
            System.err.println("Status: " + e.getStatusCode());
            System.err.println("Response body: " + e.getResponseBodyAsString());
            throw e;
        }
    }

    private static String must(String v) {
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required config value");
        }
        return v;
    }
}