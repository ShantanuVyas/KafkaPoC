package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream("app.properties")) {
            if (input == null) {
                throw new RuntimeException("app.properties not found in resources directory!");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public static String get(String key) {
        return properties.getProperty(key);
    }

    public static int getInt(String key, int defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }
}