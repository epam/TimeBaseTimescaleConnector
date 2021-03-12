package deltix.timebase.connector.config;

import deltix.gflog.Log;
import deltix.gflog.LogFactory;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;

public class TimeBaseConfig {

    private static final Log LOG = LogFactory.getLog(TimeBaseConfig.class);

    private static final String TIMEBASE_IMAGE = "registry-dev.deltixhub.com/quantserver.docker/timebase/server:6.0.9";
    private static final String TIMEBASE_SERIAL_NUMBER_SYSTEM_VAR_NAME = "TIMEBASE_SERIAL_NUMBER";

    private static final GenericContainer timebase = new GenericContainer(TIMEBASE_IMAGE)
            .withExposedPorts(8011)
            .withCopyFileToContainer(attachLicense(), "/timebase-server/inst.properties");

    public static void start() {
        timebase.start();
    }

    public static void stop() {
        timebase.stop();
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            LOG.info().append("TimeBase started at ")
                    .append(String.format("dxtick://%s:%s", timebase.getContainerIpAddress(), timebase.getFirstMappedPort()))
                    .commit();

            TestPropertyValues.of("timebase.url=" + String.format("dxtick://%s:%s", timebase.getContainerIpAddress(), timebase.getFirstMappedPort()))
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

    private static MountableFile attachLicense() {
        try {
            Path license = Files.createTempFile("inst", ".properties");

            final String serialNumber = System.getenv(TIMEBASE_SERIAL_NUMBER_SYSTEM_VAR_NAME);
            final String fileContent = String.format("serial=%s", serialNumber);

            Files.write(license, fileContent.getBytes());

            return MountableFile.forHostPath(license);
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex);
        }
    }
}
