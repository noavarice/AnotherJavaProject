package com.company;

import com.company.generators.DataGenerator;

public class Main {

    private static final int ARGS_COUNT = 2;

    public static void main(String[] args)
    {
        if (args.length != ARGS_COUNT) {
            System.out.println("Invalid arguments count");
        }
        try {
            DataGenerator.generateDatabase(args[0], args[1]);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
