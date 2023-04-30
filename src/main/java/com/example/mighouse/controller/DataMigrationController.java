package com.example.mighouse.controller;

import com.example.mighouse.service.DataMigrationService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class DataMigrationController {
    @GetMapping("/")
    public String index() {
        return "index";
    }

    @PostMapping("/migrate")
    public String migrateData(@RequestParam String sourceDbType,
                              @RequestParam String sourceHost,
                              @RequestParam String sourcePort,
                              @RequestParam String sourceDatabase,
                              @RequestParam String sourceUser,
                              @RequestParam String sourcePassword,
                              @RequestParam String clickHouseHost,
                              @RequestParam String clickHousePort,
                              @RequestParam String clickHouseDatabase,
                              @RequestParam String clickHouseUser,
                              @RequestParam String clickHousePassword,
                              @RequestParam String sourceTable,
                              @RequestParam String clickHouseTable,
                              Model model) {

        DataMigrationService dataMigrationService = new DataMigrationService();
        boolean success = dataMigrationService.migrateData(sourceDbType, sourceHost, sourcePort, sourceDatabase, sourceUser, sourcePassword, clickHouseHost, clickHousePort, clickHouseDatabase, clickHouseUser, clickHousePassword, sourceTable, clickHouseTable);

        model.addAttribute("success", success);
        return "result";
    }
}
