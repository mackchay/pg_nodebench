package com.haskov;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class ReportGenerator {

    public void generate(List<String> tableScripts, List<String> queries) {
        generateFile("queries.txt", queries);
        generateFile("tables.txt", tableScripts);
    }

    private void generateFile(String fileName, List<String> data) {

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String query : data) {
                writer.write(query);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot write to file " + fileName, e);
        }
    }
}
