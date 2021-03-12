package deltix.timebase.connector.model;

import deltix.qsrv.hf.pub.md.IntegerDataType;
import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class BusinessGroup {

    private long groupId;
    private String groupName;

    @SchemaElement
    @SchemaType(
            encoding = IntegerDataType.ENCODING_INT64,
            dataType = SchemaDataType.INTEGER
    )
    public long getGroupId() {
        return groupId;
    }

    public void setGroupId(long groupId) {
        this.groupId = groupId;
    }

    @SchemaElement
    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }
}
