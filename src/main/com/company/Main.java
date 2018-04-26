package main.com.company;

import main.com.company.generators.DataGenerator;

public class Main {

    private static final int ARGS_COUNT = 2;

    public static void main(String[] args)
    {
        if (args.length != ARGS_COUNT) {
            System.out.println("Invalid arguments count, needs " + ARGS_COUNT);
            return;
        }
        try {
            System.out.println(DataGenerator.generateDatabase(args[0], args[1]) ? "Database was generated" : "Database was not generated");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
