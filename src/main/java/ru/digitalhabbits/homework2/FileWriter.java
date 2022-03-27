package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Exchanger;
import java.nio.file.Files;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private final Exchanger<String> exchanger;
    private final Path path;
    private boolean isActive = true;


    public FileWriter(@Nonnull Exchanger<String> exchanger, @Nonnull Path path) {
        this.exchanger = exchanger;
        this.path = path;
        try {
            Files.delete(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void disable() {
        isActive=false;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        while (isActive) {
            try {
                String line = exchanger.exchange("");
                logger.info("writer thread {}", line);
                Files.write(path, line.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                Thread.sleep(200);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
