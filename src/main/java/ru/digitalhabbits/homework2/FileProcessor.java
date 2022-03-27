package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();
    public static final ExecutorService executorService =  Executors.newFixedThreadPool(CHUNK_SIZE);
    public static final Exchanger exchanger = new Exchanger();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);
        //checkFileExists(resultFileName);

        final File file = new File(processingFileName);
        // TODO: NotImplemented: запускаем FileWriter в отдельном потоке
        final FileWriter fw = new FileWriter(exchanger, Paths.get(resultFileName));
        executorService.execute(fw);
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                // TODO: NotImplemented: вычитываем CHUNK_SIZE строк для параллельной обработки
                List<Callable<Pair<String, Integer>>> tasks = new ArrayList<>();
                for (int i = 0; scanner.hasNext() && i < CHUNK_SIZE; i++) {
                    String line = scanner.nextLine();
                    logger.info(i + " - " + line);
                    tasks.add(() -> {
                        // TODO: NotImplemented: обрабатывать строку с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                        return new LineCounterProcessor().process(line);
                    });
                }

                // TODO: NotImplemented: добавить обработанные данные в результирующий файл
                try {
                    var t =  executorService.invokeAll(tasks);
                    for (var f : t) {
                        exchanger.exchange(f.get().getKey() + " " + f.get().getValue() + "\n");
                    }
                    logger.info("task done!");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }

            }
        } catch (IOException exception) {
            logger.error("", exception);
        }

        // TODO: NotImplemented: остановить поток writerThread
        fw.disable();
        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
