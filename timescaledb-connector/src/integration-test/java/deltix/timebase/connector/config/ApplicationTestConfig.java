package deltix.timebase.connector.config;

import deltix.timebase.connector.ApplicationEventHandler;
import deltix.timebase.connector.ApplicationStarter;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@TestConfiguration
@Import(ApplicationStarter.class)
public class ApplicationTestConfig {

    @MockBean
    private ApplicationEventHandler eventHandler;
}
