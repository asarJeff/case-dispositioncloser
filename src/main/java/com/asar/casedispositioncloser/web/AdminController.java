package com.asar.casedispositioncloser.web;

import com.asar.casedispositioncloser.service.CaseDispositionCloseService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final CaseDispositionCloseService service;

    public AdminController(CaseDispositionCloseService service) {
        this.service = service;
    }

    @GetMapping("/ping")
    public String ping() {
        return "case-disposition-closer is running";
    }

    @PostMapping("/run-now")
    public ResponseEntity<String> runNow() {
        service.processOnce();
        return ResponseEntity.ok("Disposition close run completed.");
    }
}