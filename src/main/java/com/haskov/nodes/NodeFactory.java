package com.haskov.nodes;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class NodeFactory {
    private static final Map<String, Class<? extends Node>> nodeMap = new HashMap<>();

    static {
        nodeMap.put("SeqScan", SeqScan.class);
    }

    // Метод для создания узла
    public static Node createNode(String nodeType) {
        Class<? extends Node> nodeClass = nodeMap.get(nodeType.toLowerCase());

        if (nodeClass == null) {
            throw new IllegalArgumentException("Unknown node type: " + nodeType);
        }

        try {
            return nodeClass.getDeclaredConstructor().newInstance(); // Создаем объект через рефлексию
        } catch (Exception e) {
            throw new RuntimeException("Error creating node instance", e);
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter the type of node (select, insert, update, delete): ");
        String nodeType = scanner.nextLine();

        try {
            Node node = NodeFactory.createNode(nodeType);

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }
    }
}
