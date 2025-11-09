package org.flinkdsl.produce;

public final class Customer {
    public String id;
    public String firstName;
    public String lastName;
    public String email;
    public long createdAtEpochMs;

    public Customer(
            String id,
            String firstName,
            String lastName,
            String email,
            long createdAtEpochMs
    ) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.createdAtEpochMs = createdAtEpochMs;
    }
}

