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

        int solved = 0;
        int skippedBlankDisposition = 0;
        int skippedNotActive = 0;
        int skippedAlreadySolved = 0;
        int failed = 0;

        for (int page = 0; page < maxPages; page++) {
            int skip = page * pageSize;

            System.out.printf("Fetching cases page=%d skip=%d top=%d%n", page, skip, pageSize);

            JsonNode root = api.getCases(pageSize, skip);
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

                if (disposition == null || disposition.isBlank()) {
                    skippedBlankDisposition++;
                    System.out.printf("SKIP case %s : Disposition is blank.%n", displayId);
                    continue;
                }

                // Only skip if already solved
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