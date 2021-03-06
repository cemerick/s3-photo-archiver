package com.cemerick.s3photo;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Archiver {
    public static final Set<String> DEFAULT_FILE_TYPES = Collections.unmodifiableSet(
            new HashSet<String>(Arrays.asList(new String[] {
                    // images
                    "jpg", "jpeg", "png", "gif", "bmp", "tiff",
                    // video
                    "mp4", "mov", "avi", "mpeg", "mts"})));

    private static Pattern fileExtPattern = Pattern.compile("(?<=\\.)([^\\.]+)$");

    private final AWSCredentials awsCreds;
    private final String s3BucketName;
    private final File[] sources;

    public Archiver (AWSCredentials awsCreds, String s3BucketName, File... sources) {
        assert awsCreds != null;
        assert s3BucketName != null;
        assert sources.length > 0;

        for (File f : sources) assert f.exists();

        this.awsCreds = awsCreds;
        this.s3BucketName = s3BucketName;
        this.sources = sources;
    }

    private static String fileExtension (File f) {
        Matcher m = fileExtPattern.matcher(f.getName());
        return m.find() ? m.group(1).toLowerCase() : null;
    }

    public void run () throws IOException, InterruptedException {
        final ArrayList<File> media = new ArrayList<File>();

        for (File source : sources) {
            Files.walkFileTree(source.toPath(), new SimpleFileVisitor<Path>() {
                public FileVisitResult visitFile (Path file, BasicFileAttributes attrs)
                        throws IOException {
                    File f = file.toFile();
                    if (!f.isHidden()) {
                        String type = fileExtension(f);
                        if (type == null || !DEFAULT_FILE_TYPES.contains(type)) {
                            System.err.printf("Unknown file type, ignoring: %s\n", f.getCanonicalPath());
                        } else {
                            media.add(f);
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        }

        System.out.printf("Archiving %s files to s3://%s\n", media.size(), s3BucketName);

        final AmazonS3Client s3 = new AmazonS3Client(awsCreds);

        ExecutorService exec = Executors.newFixedThreadPool(8);
        final AtomicInteger completed = new AtomicInteger();
        final HashMap<File, String> skipped = new HashMap<File, String>();
        final ArrayList<File> failed = new ArrayList<File>();

        for (final File f : media) {
            exec.execute(new Runnable () {
                public void run () {
                    try {
                        String s3key = (DigestUtils.sha1Hex(new BufferedInputStream(new FileInputStream(f))) + "." +
                                fileExtension(f)).toLowerCase();
                        try {
                            ObjectMetadata om = s3.getObjectMetadata(s3BucketName, s3key);
                            if (om.getContentLength() != f.length()) {
                                // odds of a hash collision where the files are of different lengths?
                                System.err.printf("[ERROR] likely hash collision found, %s (%s) -> %s (%s)\n",
                                        f.getAbsolutePath(), f.length(), s3key, om.getContentLength());
                                failed.add(f);
                            } else {
                                skipped.put(f, s3key);
                            }
                        } catch (AmazonS3Exception e) {
                            if (e.getStatusCode() == 404) {
                                s3.putObject(new PutObjectRequest(s3BucketName, s3key, f)
                                        .withStorageClass(StorageClass.StandardInfrequentAccess));
                                completed.incrementAndGet();
                            } else {
                                throw e;
                            }
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        failed.add(f);
                    }

                    System.out.print(".");
                    System.out.flush();
                }
            });
        }

        exec.shutdown();
        exec.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        System.out.println();
        if (skipped.size() > 0) {
            System.out.println("----------------------------------");
            System.out.println("Skipped:");
            for (Map.Entry<File, String> e : new ArrayList<Map.Entry<File, String>>(skipped.entrySet())) {
                System.out.printf("%s -> %s\n", e.getKey().getCanonicalPath(), e.getValue());
            }
        }
        if (failed.size() > 0) {
            System.out.println("----------------------------------");
            System.out.println("Failed:");
            for (File f : failed) System.out.println(f.getCanonicalPath());
        }

        System.out.printf("\n%s Archived %s, skipped %s, %s failures.\n", failed.size() > 0 ? "!ERRORS!" : "Done.",
                completed.get(), skipped.size(), failed.size());

        System.exit(failed.size() == 0 ? 0 : 1);
    }

    private static String findConfig (String configKey) {
        String value = System.getProperty(configKey, System.getenv(configKey));
        if (value == null) throw new IllegalStateException(
                String.format("No value found for %s in environment or system properties, cannot proceed.",
                        configKey));
        return value;
    }

    private static void badarg (String msg) {
        throw new IllegalArgumentException(msg);
    }

    public static void main (String[] args) throws Throwable {
        if (args.length == 0) badarg("Arguments must be upload sources.");
        File[] sources = new File[args.length];
        for (int i = 0; i < args.length; i++) {
            sources[i] = new File(args[i]);
            if (!sources[i].exists()) badarg(String.format("%s does not exist", args[i]));
        }
        new Archiver(new BasicAWSCredentials(findConfig("AWS_ACCESS_KEY_ID"), findConfig("AWS_SECRET_ACCESS_KEY")),
                findConfig("AWS_S3_BUCKET"),
                sources).run();
    }
}
