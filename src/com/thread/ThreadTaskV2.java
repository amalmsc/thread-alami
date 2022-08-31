package com.thread;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

public class ThreadTaskV2 {
    public static void main(String[] args) {
        List<String> nameList = new ArrayList<>(), thread_1 = new ArrayList<>(), thread_2a = new ArrayList<>(),
                thread_2b = new ArrayList<>(), thread_3 = new ArrayList<>();
        List<Integer> idList = new ArrayList<>(), ageList = new ArrayList<>(), freeTransferList = new ArrayList<>();
        List<Float> balancedList = new ArrayList<>(), prevBalancedList = new ArrayList<>(),
                avgBalancedList = new ArrayList<>();
        try {
            Scanner scanner = new Scanner(new File("Before Eod.csv"));
            scanner.useDelimiter(";\\r\\n|;|\\r\\n");
            scanner.nextLine();
            while (scanner.hasNext()) {
                idList.add(Integer.parseInt(scanner.next()));
                nameList.add(scanner.next());
                ageList.add(Integer.parseInt(scanner.next()));
                balancedList.add(Float.parseFloat(scanner.next()));
                prevBalancedList.add(Float.parseFloat(scanner.next()));
                avgBalancedList.add(Float.parseFloat(scanner.next()));
                freeTransferList.add(Integer.parseInt(scanner.next()));

                thread_1.add("");
                thread_2a.add("");
                thread_2b.add("");
                thread_3.add("");
            }
            scanner.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        ForkJoinPool fjp_1 = null, fjp_2a = null, fjp_2b = null, fjp_3 = null;
        final ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> {
            final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName(String.valueOf(worker.getPoolIndex() + 1));
            return worker;
        };

        try {
            fjp_3 = new ForkJoinPool(8, factory, null, false);
            fjp_3.submit(() -> idList.parallelStream().limit(100).forEach(id -> {
                Float balanced = balancedList.get(id - 1);
                String currentThreadNo = Thread.currentThread().getName();
                balanced = balanced + 10;
                balancedList.set(id - 1, balanced);
                thread_3.set(id - 1, currentThreadNo);
            })).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (fjp_3 != null) {
                fjp_3.shutdown();
            }
        }

        try {
            fjp_2a = new ForkJoinPool(3, factory, null, false);
            fjp_2a.submit(() -> idList.parallelStream().forEach(id -> {
                Float balanced = balancedList.get(id - 1);
                String currentThreadNo = Thread.currentThread().getName();
                thread_2a.set(id - 1, currentThreadNo);
                if (balanced >= 100 && balanced <= 150) {
                    freeTransferList.set(id - 1, 5);
                }
            })).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (fjp_2a != null) {
                fjp_2a.shutdown();
            }
        }

        try {
            fjp_2b = new ForkJoinPool(3, factory, null, false);
            fjp_2b.submit(() -> idList.parallelStream().forEach(id -> {
                Float balanced = balancedList.get(id - 1);
                String currentThreadNo = Thread.currentThread().getName();
                thread_2b.set(id - 1, currentThreadNo);
                if (balanced >= 150) {
                    balanced = balanced + 25;
                    balancedList.set(id - 1, balanced);
                }
            })).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (fjp_2b != null) {
                fjp_2b.shutdown();
            }
        }

        try {
            fjp_1 = new ForkJoinPool(3, factory, null, false);
            fjp_1.submit(() -> idList.parallelStream().forEach(id -> {
                Float balanced = balancedList.get(id - 1);
                Float prevBalanced = prevBalancedList.get(id - 1);
                Float avgBalanced = (balanced + prevBalanced) / 2;
                String currentThreadNo = Thread.currentThread().getName();
                avgBalancedList.set(id - 1, avgBalanced);
                thread_1.set(id - 1, currentThreadNo);
            })).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            if (fjp_1 != null) {
                fjp_1.shutdown();
            }
        }

        List<String[]> dataLines = new ArrayList<>(Collections.singletonList(new String[]{"id", "Nama", "Age",
                "Balanced", "No 2b Thread-No", "No 3 Thread-No", "Previous Balanced", "Average Balanced",
                "No 1 Thread-No", "Free Transfer", "No 2a Thread-No"}));

        idList.forEach(id -> dataLines.add(new String[] {
                String.valueOf(idList.get(id-1)), nameList.get(id-1), String.valueOf(ageList.get(id-1)),
                String.valueOf(balancedList.get(id-1)), thread_2b.get(id-1), thread_3.get(id-1),
                String.valueOf(prevBalancedList.get(id-1)), String.valueOf(avgBalancedList.get(id-1)),
                thread_1.get(id-1), String.valueOf(freeTransferList.get(id-1)), thread_2a.get(id-1)
        }));

        File csvOutputFile = new File("After Eod V2.csv");
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            dataLines.stream()
                    .map(dateline -> String.join(";", dateline))
                    .forEach(pw::println);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}