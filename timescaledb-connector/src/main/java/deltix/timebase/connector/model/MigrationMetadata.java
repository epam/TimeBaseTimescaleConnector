package deltix.timebase.connector.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class MigrationMetadata {

    private Integer id;
    private String stream;
    private Long version;
    private Boolean isSuccess;
    private Long dateTime;
}
