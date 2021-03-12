package deltix.timebase.connector.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import deltix.timebase.connector.service.timebase.TimebaseConnectionService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableScheduling
public class ApplicationConfig {

    @ConfigurationProperties(prefix = "timebase.streams")
    @Bean
    public List<String> streamNames() {
        return new ArrayList<>();
    }

    @Bean
    public TimebaseConnectionService timebaseConnectionService(@Value("${timebase.url}") String timebaseUrl) {
        return new TimebaseConnectionService(timebaseUrl);
    }

    @Bean
    public ObjectMapper mapper() {
        return new ObjectMapper();
    }

    @Bean
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(JdbcTemplate template) {
        return new NamedParameterJdbcTemplate(template);
    }

}
