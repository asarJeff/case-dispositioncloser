package com.asar.casedispositioncloser.service;

import com.asar.casedispositioncloser.api.CaseApiClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class CaseDispositionCloseService {

    private final CaseApiClient api;
    private final ObjectMapper om;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${close.enabled:true}")
    private boolean enabled;

    @Value("${close.page-size:200}")
    private int pageSize;

    @Value("${close.max-pages:50}")
    private int maxPages;

    @Value("${close.disposition-field:Disposition}")
    private String dispositionField;

    @Value("${close.status-schema:01}")
    private String statusSchema;

    @Value("${close.status-solved:Z5}")
    private String statusSolved;

    @Value("${close.lifecycle-open:OPEN}")
    private String lifecycleOpen;

    // Statuses we care about — cases already solved don't need processing
    // Configurable so we can adjust without code changes
    @Value("${close.active-statuses:Z2,Z3,01,Z7,Z8,Z6,Z1}")
    private String activeStatusesCsv;

    public CaseDispositionCloseService(CaseApiClient api, ObjectMapper om) {
        this.api = api;
        this.om = om;
    }

    @Scheduled(fixedDelayString = "${close.fixed-delay-ms:300000}")
    public void scheduled() {
        if (!enabled) {
            return;
        }

        if (!running.compareAndSet(false, true)) {
            System.out.println("Disposition closer already running. Skipping overlapping run.");
            return;
        }

        try {
            processOnce();
        } finally {
            running.set(false);
        }
    }

    public void processOnce() {
        long start = System.currentTimeMillis();

        System.out.println("==================================================");
        System.out.println("Disposition Closer - processOnce START");
        System.out.println("==================================================");

        // Build SAP-side filter — only fetch cases that:
        // 1. Are not already solved (status ne statusSolved)
        // 2. Have a disposition value set (extensions/Disposition ne '')
        // This avoids fetching all cases and hitting the SAP 10k $skip limit
        String filter = buildFilter();
        System.out.println("Disposition Closer: using SAP filter: " + filter);

        int solved = 0;
        int skippedBlankDisposition = 0;
        int skippedNotActive = 0;
        int skippedAlreadySolved = 0;
        int failed = 0;

        for (int page = 0; page < maxPages; page++) {
            int skip = page * pageSize;

            // SAP hard limit — never go past 10k skip
            if (skip >= 10000) {
                System.out.println("WARNING: hit SAP 10k $skip limit at page=" + page
                        + ". Total solved so far: " + solved
                        + ". Consider narrowing filter further.");
                break;
            }

            System.out.printf("Fetching cases page=%d skip=%d top=%d%n", page, skip, pageSize);

            JsonNode root = api.getCasesWithFilter(pageSize, skip, filter);
            JsonNode value = (root == null) ? null : root.get("value");

            if (value == null || !value.isArray() || value.size() == 0) {
                System.out.println("No more cases returned, stopping pagination.");
                break;
            }

            for (JsonNode c : value) {
                String caseId = text(c.get("id"));
                String displayId = text(c.get("displayId"));
                String status = text(c.get("status"));
                String disposition = text(c.at("/extensions/" + dispositionField));

                if (caseId == null || caseId.isBlank()) {
                    continue;
                }

                // Safety net — SAP filter should have excluded these
                // but keep the guard in case SAP filter behaves unexpectedly
                if (disposition == null || disposition.isBlank()) {
                    skippedBlankDisposition++;
                    System.out.printf("SKIP case %s : Disposition is blank.%n", displayId);
                    continue;
                }

                if (statusSolved.equals(status)) {
                    skippedAlreadySolved++;
                    System.out.printf("SKIP case %s : already solved.%n", displayId);
                    continue;
                }

                try {
                    solveCase(caseId);
                    solved++;
                    System.out.printf("SUCCESS case %s : Disposition present, case moved to Solved.%n", displayId);

                } catch (Exception ex) {
                    failed++;
                    System.err.printf("FAILED case %s : %s%n", displayId, ex.getMessage());
                }
            }
        }

        long ms = System.currentTimeMillis() - start;

        System.out.println("--------------------------------------------------");
        System.out.println("Disposition Solver Run Summary");
        System.out.println("--------------------------------------------------");
        System.out.printf("Solved: %d%n", solved);
        System.out.printf("Skipped - Blank Disposition: %d%n", skippedBlankDisposition);
        System.out.printf("Skipped - Not Active Status: %d%n", skippedNotActive);
        System.out.printf("Skipped - Already Solved: %d%n", skippedAlreadySolved);
        System.out.printf("Failed: %d%n", failed);
        System.out.printf("Elapsed: %d ms%n", ms);
        System.out.println("--------------------------------------------------");
    }

    /**
     * Builds an OData $filter to fetch only cases that need processing:
     * - Not already solved
     * - Has an active-like status
     * We can't filter on extensions/Disposition directly via OData
     * but filtering on status alone cuts volume dramatically.
     */
    private String buildFilter() {
        StringBuilder sb = new StringBuilder();
        sb.append("status ne '").append(statusSolved).append("'");
        return sb.toString();
    }

    private void solveCase(String caseId) throws Exception {
        JsonNode current = api.getCaseById(caseId);

        String currentStatus = text(current.get("status"));
        String currentSchema = text(current.get("statusSchema"));

        if (statusSolved.equals(currentStatus)) {
            return;
        }

        String schemaToUse = (currentSchema == null || currentSchema.isBlank()) ? statusSchema : currentSchema;

        ObjectNode toSolved = om.createObjectNode();
        toSolved.put("statusSchema", schemaToUse);
        toSolved.put("status", statusSolved);
        toSolved.put("lifeCycleStatus", lifecycleOpen);

        api.patchCaseWithEtag(caseId, toSolved);

        System.out.printf("Case %s moved to Solved (%s).%n", caseId, statusSolved);
    }

    private static String text(JsonNode n) {
        if (n == null || n.isMissingNode() || n.isNull()) {
            return null;
        }
        String v = n.asText();
        return v == null ? null : v.trim();
    }
}