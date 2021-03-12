package deltix.timebase.connector.model;

import deltix.timebase.messages.SchemaDataType;
import deltix.timebase.messages.SchemaElement;
import deltix.timebase.messages.SchemaType;

public class OwnerEntity {

    private String name;
    private String email;
    private int priority;
    private BusinessGroup businessGroup;

    @SchemaElement
    public BusinessGroup getBusinessGroup() {
        return businessGroup;
    }

    public void setBusinessGroup(BusinessGroup businessGroup) {
        this.businessGroup = businessGroup;
    }

    @SchemaElement
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @SchemaElement
    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @SchemaType(
            encoding = "INT32",
            dataType = SchemaDataType.INTEGER
    )
    @SchemaElement
    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }
}
