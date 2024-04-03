package com.google.common.util.concurrent;

import junit.framework.TestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class CloseableListTest extends TestCase {
    public void testEqualsEmpty() {
        CloseableList list1 = new CloseableList();
        CloseableList list2 = new CloseableList();
        boolean result=list1.equals(list2);
        assertTrue(result);

    }

    public void testEqualsWhenClose() {
        CloseableList list1 = new CloseableList();
        CloseableList list2 = new CloseableList();
        list1.close();
        boolean result=list1.equals(list2);
        assertFalse(result);
    }

    public void testEqualsWhenAdd() {
        CloseableList list1 = new CloseableList();
        CloseableList list2 = new CloseableList();
        list1.add(() -> {}, Executors.newSingleThreadExecutor());
        boolean result=list1.equals(list2);
        assertFalse(result);
    }

    public void testApplyClosingFunction() throws Exception {
        CloseableList list = new CloseableList();
        int input = 5;
        ClosingFunction<Integer, String> function = (closer, value) -> {
            list.add(() -> {}, Executors.newSingleThreadExecutor());
            return "Result: " + value;
        };

        ListenableFuture<String> future = list.applyClosingFunction(function, input);
        assertEquals("Result: 5", future.get());
    }

    public void testClose() {
        CloseableList list = new CloseableList();
        list.add(() -> {}, Executors.newSingleThreadExecutor());
        list.add(() -> {}, Executors.newSingleThreadExecutor());
        list.close();

        assertTrue(list.isEmpty());
    }

    public void testAdd() {
        CloseableList list = new CloseableList();
        Executor executor = Executors.newSingleThreadExecutor();
        list.add(() -> {}, executor);

        assertFalse(list.isEmpty());
        assertEquals(1, list.size());
    }

    public void testWhenClosedCountDown() {
        CloseableList list = new CloseableList();
        CountDownLatch latch = list.whenClosedCountDown();
        assertFalse(latch.getCount() == 0);
        list.close();
        assertEquals(0, latch.getCount());
    }



}
